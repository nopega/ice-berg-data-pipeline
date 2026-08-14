"""
gold: the table Power BI actually connects to, one day at a time.

    spark-submit gold_aggregate.py --date 2024-01-15

SHAPED FOR THE DASHBOARD, NOT FOR THE WAREHOUSE
-------------------------------------------------
Power BI can talk to Trino two ways. DirectQuery sends a fresh SQL statement
every time someone clicks a slicer, which puts an interactive latency budget on
a query engine sized for analytics. Import loads once and everything after that
happens in memory on the report.

Import is the right mode here, and it is only viable if the table is small and
needs no joins. So this table is:

  - PRE-AGGREGATED to day x pickup zone x payment method. A day of raw trips is
    ~100,000 rows; this is a few hundred. A year of it still imports instantly.
  - PRE-JOINED. The zone lookup is resolved here, so the report has borough and
    zone NAMES as plain columns. A Power BI user never has to know that
    LocationID 132 is JFK, and never has to model a relationship.
  - PRE-COMPUTED for the ratios that get mis-averaged otherwise. tip_pct is
    stored as total_tip / total_fare for the group. If the report averaged a
    per-trip percentage instead, a $4 trip with a $2 tip would count as much as
    a $200 trip with a $10 tip, and the headline number would be wrong in a way
    that looks plausible.

Because it is written one day at a time, a correction to a single day republishes
that day alone -- the dashboard does not have to be rebuilt to fix a Tuesday.
"""

from __future__ import annotations

import csv
import io
import os
import sys

import requests
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    GOLD,
    PARTITION_COL,
    SILVER,
    TLC_BASE,
    build_spark,
    date_arg,
    fail,
    replace_day,
)

ZONE_LOOKUP_URL = f"{TLC_BASE}/misc/taxi_zone_lookup.csv"


def load_zones(spark):
    """
    265 rows describing every LocationID. Small enough to fetch on the driver
    and broadcast, which turns the join below into a map-side lookup with no
    shuffle at all.
    """
    print(f"[2/5] Fetching zone lookup from {ZONE_LOOKUP_URL}")
    try:
        resp = requests.get(ZONE_LOOKUP_URL, timeout=(10, 60))
        resp.raise_for_status()
    except requests.RequestException as exc:
        fail(f"could not fetch the zone lookup: {exc}")

    rows = list(csv.DictReader(io.StringIO(resp.text)))
    if not rows:
        fail("zone lookup came back empty")

    zones = spark.createDataFrame(
        [
            (
                int(r["LocationID"]),
                (r.get("Borough") or "Unknown").strip(),
                (r.get("Zone") or "Unknown").strip(),
            )
            for r in rows
            if r.get("LocationID", "").isdigit()
        ],
        "location_id INT, borough STRING, zone STRING",
    )
    print(f"      {zones.count()} zones")
    return F.broadcast(zones)


def main() -> None:
    day = date_arg(sys.argv)
    spark = build_spark(f"taxi-gold-{day.isoformat()}")

    print(f"[1/5] Reading {SILVER} for {day}...")
    if not spark.catalog.tableExists(SILVER):
        fail(f"{SILVER} does not exist. The silver task has not run.")
    trips = spark.table(SILVER).where(F.col(PARTITION_COL) == day.isoformat())
    n = trips.count()
    if n == 0:
        fail(f"silver has no rows for {day}")
    print(f"      {n:,} trips")

    zones = load_zones(spark)

    print("[3/5] Aggregating to zone x payment method...")
    # left join, not inner: a LocationID missing from the lookup should show up
    # as "Unknown" in the dashboard rather than vanish. Trips disappearing
    # between silver and gold is the kind of error only ever noticed as a total
    # that does not tie out.
    joined = (
        trips.join(zones, trips.pickup_location_id == zones.location_id, how="left")
        .withColumn("pickup_zone", F.coalesce(F.col("zone"), F.lit("Unknown")))
        .withColumn("pickup_borough", F.coalesce(F.col("borough"), F.lit("Unknown")))
    )

    agg = (
        joined.groupBy(PARTITION_COL, "pickup_zone", "pickup_borough", "payment_method")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("passenger_count").alias("passenger_count"),
            F.round(F.sum("trip_distance_mi"), 2).alias("total_distance_mi"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tip"),
            F.round(F.sum("tolls_amount"), 2).alias("total_tolls"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance_mi"), 2).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed_mph"),
        )
        # Ratio of sums, not average of ratios -- see the module docstring.
        .withColumn(
            "tip_pct",
            F.round(
                F.when(F.col("total_fare") > 0, F.col("total_tip") / F.col("total_fare") * 100).otherwise(0.0),
                2,
            ),
        )
    )

    out = agg.select(
        PARTITION_COL,
        "pickup_borough",
        "pickup_zone",
        "payment_method",
        "trip_count",
        "passenger_count",
        "total_distance_mi",
        "total_fare",
        "total_tip",
        "total_tolls",
        "total_revenue",
        "avg_fare",
        "avg_distance_mi",
        "avg_duration_min",
        "avg_speed_mph",
        "tip_pct",
    )

    rows = out.count()
    print(f"      {rows:,} aggregate rows")

    # The whole Import-mode argument depends on this staying small. If a change
    # upstream ever explodes the grain, this is where it should stop.
    if rows > 20_000:
        fail(
            f"{rows:,} rows for a single day is far more than this grain should "
            f"produce (a few hundred). Something upstream changed; refusing to "
            f"publish a gold table Power BI cannot import."
        )

    print("[4/5] Writing...")
    replace_day(spark, GOLD, day)
    if spark.catalog.tableExists(GOLD):
        out.writeTo(GOLD).append()
    else:
        out.writeTo(GOLD).partitionedBy(PARTITION_COL).createOrReplace()

    print("[5/5] Reconciling against silver...")
    # Trip counts must tie out exactly. If they do not, the join dropped rows,
    # and a revenue dashboard built on it would be quietly understated.
    gold_trips = (
        spark.table(GOLD)
        .where(F.col(PARTITION_COL) == day.isoformat())
        .agg(F.sum("trip_count"))
        .collect()[0][0]
        or 0
    )
    if int(gold_trips) != n:
        fail(f"gold totals {int(gold_trips):,} trips but silver has {n:,} -- rows were lost in the join")
    print(f"      {int(gold_trips):,} trips reconciled")

    top = out.groupBy("pickup_borough").agg(F.sum("total_revenue").alias("rev")).orderBy(F.desc("rev"))
    print(f"\n      revenue by borough on {day}:")
    for r in top.collect():
        print(f"        {r['pickup_borough']:<16} {r['rev']:>13,.2f}")

    print(f"\n      {GOLD} ready for Power BI")
    spark.stop()


if __name__ == "__main__":
    main()
