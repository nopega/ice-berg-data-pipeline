"""
silver: one clean, typed, de-duplicated row per real trip, for one day.

    spark-submit silver_clean.py --date 2024-01-15

EVERY FILTER HERE IS A CHOICE, SO EVERY FILTER IS COUNTED
-----------------------------------------------------------
The job prints how many rows each rule removed. That matters more than it
looks: a cleaning rule that silently starts dropping 40% of a day is
indistinguishable, downstream, from a quiet Tuesday. Printing the counts turns
a data-quality regression into something visible in the Airflow log on the day
it happens.

Nothing is deleted from bronze. Silver is derived, and a filter that turns out
to be wrong is fixed by re-running this task, not by re-downloading.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    BRONZE,
    PARTITION_COL,
    SILVER,
    build_spark,
    date_arg,
    fail,
    replace_day,
)

# Rules, in the order they are applied. Each is (name, condition-to-KEEP).
#
# The bounds are not arbitrary:
#   - a pickup outside the partition's own day is a corrupt timestamp; TLC
#     files routinely contain a handful of rows dated 2001 or 2098
#   - distance and fare of exactly 0 are cancelled or voided trips
#   - a negative fare is a refund posted as a trip
#   - over 500 miles inside NYC is a meter fault
#   - a duration over 24h or under 1 minute is a meter left running or a
#     mis-punch, not a journey
RULES = [
    ("pickup timestamp missing", F.col("tpep_pickup_datetime").isNotNull()),
    ("dropoff timestamp missing", F.col("tpep_dropoff_datetime").isNotNull()),
    ("dropoff before pickup", F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")),
    ("pickup outside the partition day", F.to_date("tpep_pickup_datetime") == F.col(PARTITION_COL)),
    ("duration under 1 minute", F.col("trip_duration_min") >= 1),
    ("duration over 24 hours", F.col("trip_duration_min") <= 1440),
    ("distance not positive", F.col("trip_distance") > 0),
    ("distance over 500 miles", F.col("trip_distance") <= 500),
    ("fare not positive", F.col("fare_amount") > 0),
    ("total below fare", F.col("total_amount") >= F.col("fare_amount")),
    ("negative tip", F.col("tip_amount") >= 0),
]


def main() -> None:
    day = date_arg(sys.argv)
    spark = build_spark(f"taxi-silver-{day.isoformat()}")

    print(f"[1/5] Reading {BRONZE} for {day}...")
    if not spark.catalog.tableExists(BRONZE):
        fail(f"{BRONZE} does not exist. The bronze task has not run.")

    src = spark.table(BRONZE).where(F.col(PARTITION_COL) == day.isoformat())
    raw_count = src.count()
    if raw_count == 0:
        fail(f"bronze has no rows for {day}. Did the bronze task write a different date?")
    print(f"      {raw_count:,} rows")

    print("[2/5] Deriving columns...")
    df = (
        src.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow", F.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn(
            "trip_duration_min",
            (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0,
        )
    )

    print("[3/5] Applying quality rules...")
    kept = df
    for name, keep in RULES:
        before = kept.count()
        kept = kept.where(keep)
        removed = before - kept.count()
        pct = (removed / raw_count * 100) if raw_count else 0
        flag = "  <-- unusually high" if pct > 10 else ""
        print(f"      {name:<34} removed {removed:>8,} ({pct:5.2f}%){flag}")

    # Dedupe AFTER filtering: a duplicate of a row that was going to be dropped
    # anyway is not worth the shuffle. Two trips can legitimately share a
    # pickup time and location, so the key includes the fare and the dropoff.
    before = kept.count()
    kept = kept.dropDuplicates(
        [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "total_amount",
        ]
    )
    after = kept.count()
    print(f"      {'exact duplicates':<34} removed {before - after:>8,}")

    kept = kept.withColumn(
        "avg_speed_mph",
        F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
    ).withColumn(
        "payment_method",
        # The raw column is an integer code nobody can read in a dashboard.
        # Resolving it here means neither gold nor Power BI needs the mapping.
        F.when(F.col("payment_type") == 1, "Credit card")
        .when(F.col("payment_type") == 2, "Cash")
        .when(F.col("payment_type") == 3, "No charge")
        .when(F.col("payment_type") == 4, "Dispute")
        .when(F.col("payment_type") == 5, "Unknown")
        .when(F.col("payment_type") == 6, "Voided trip")
        .otherwise("Unspecified"),
    )

    final = kept.select(
        PARTITION_COL,
        "pickup_ts",
        "dropoff_ts",
        "pickup_hour",
        "pickup_dow",
        "trip_duration_min",
        "avg_speed_mph",
        F.col("PULocationID").alias("pickup_location_id"),
        F.col("DOLocationID").alias("dropoff_location_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance_mi"),
        "payment_method",
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        "_run_id",
    )

    surviving = final.count()
    pct = surviving / raw_count * 100
    print(f"      {surviving:,} of {raw_count:,} rows survive ({pct:.1f}%)")
    if pct < 50:
        fail(
            f"only {pct:.1f}% of rows survived cleaning. That is a data or rule "
            f"problem, not a normal day -- refusing to publish silver."
        )

    print(f"[4/5] Writing {SILVER}...")
    replace_day(spark, SILVER, day)
    if spark.catalog.tableExists(SILVER):
        final.writeTo(SILVER).append()
    else:
        final.writeTo(SILVER).partitionedBy(PARTITION_COL).createOrReplace()

    print("[5/5] Verifying...")
    written = spark.table(SILVER).where(F.col(PARTITION_COL) == day.isoformat()).count()
    if written != surviving:
        fail(f"expected {surviving:,} rows in silver, found {written:,}")
    print(f"      {written:,} rows in {SILVER} for {day}")
    spark.stop()


if __name__ == "__main__":
    main()
