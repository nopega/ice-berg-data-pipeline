"""
gold: one row per (event day, region), for dashboards.

    spark-submit gold_daily.py

Rebuilds the last REBUILD_DAYS days of event dates from silver, replacing
exactly those partitions (dynamic partition overwrite). That window covers the
30-day event-time window bronze reads, so any day a revision could have touched
is recomputed, and nothing older is rewritten.

Besides counts and magnitudes it reports how much USGS revised its numbers --
the thing only an SCD Type 2 history can answer:
    revised_events      events with more than one distinct state
    avg_mag_revision    mean (latest magnitude - first reported magnitude)
"""

from __future__ import annotations

import datetime as dt
import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import CURRENT, DAILY, HISTORY, build_spark, fail  # noqa: E402

REBUILD_DAYS = 35


def main() -> None:
    spark = build_spark("earthquakes-gold")
    for t in (CURRENT, HISTORY):
        if not spark.catalog.tableExists(t):
            fail(f"{t} does not exist. The silver task has not run.")

    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=REBUILD_DAYS)).date()
    print(f"[1/3] Rebuilding event days from {since}")

    cur = (spark.table(CURRENT)
           .where(F.to_date("event_time") >= F.lit(since))
           .where(F.col("event_type") == "earthquake"))

    # First and latest magnitude per event, from the SCD2 history.
    w = Window.partitionBy("event_id").orderBy("valid_from")
    hist = (spark.table(HISTORY).join(cur.select("event_id"), "event_id")
            .withColumn("_first_mag", F.first("mag").over(w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
            .groupBy("event_id")
            .agg(F.count("*").alias("states"), F.first("_first_mag").alias("first_mag")))

    out = (
        cur.join(hist, "event_id", "left")
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("region", F.coalesce(F.nullif(F.trim(F.substring_index("place", ",", -1)), F.lit("")), F.lit("Unknown")))
        .groupBy("event_date", "region")
        .agg(
            F.count("*").alias("events"),
            F.max("mag").alias("max_mag"),
            F.round(F.avg("mag"), 2).alias("avg_mag"),
            F.sum(F.when(F.col("mag") >= 5, 1).otherwise(0)).alias("m5_plus"),
            F.round(F.avg(F.when(F.col("status") == "reviewed", 1.0).otherwise(0.0)), 3).alias("reviewed_share"),
            F.sum(F.when(F.col("states") > 1, 1).otherwise(0)).alias("revised_events"),
            F.round(F.avg(F.col("mag") - F.col("first_mag")), 3).alias("avg_mag_revision"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )

    print(f"[2/3] Writing {DAILY}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DAILY} (
            event_date DATE, region STRING, events BIGINT, max_mag DOUBLE, avg_mag DOUBLE,
            m5_plus BIGINT, reviewed_share DOUBLE, revised_events BIGINT, avg_mag_revision DOUBLE,
            _built_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (event_date)
    """)
    # Replaces only the event_date partitions present in `out`; older days stay.
    out.writeTo(DAILY).overwritePartitions()

    print("[3/3] Done")
    spark.table(DAILY).where(F.col("event_date") >= F.lit(since)) \
        .groupBy("event_date").agg(F.sum("events").alias("events")) \
        .orderBy(F.col("event_date").desc()).show(7, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
