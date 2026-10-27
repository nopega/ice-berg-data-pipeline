"""
silver: SCD Type 2 history and SCD Type 1 current state, from the changelog.

    spark-submit silver_scd.py

TWO TABLES, TWO QUESTIONS
-------------------------
event_history  (SCD Type 2)  "what did we believe about this quake, and when?"
    one row per DISTINCT state: valid_from, valid_to, is_current.
    A magnitude revised 4.5 -> 4.7 is two rows; the first gets a valid_to.

event_current  (SCD Type 1)  "what is the latest we know?"
    one row per quake, overwritten in place. What dashboards read.

INCREMENTAL, AND SAFE TO RE-RUN
-------------------------------
The input is every changelog version newer than the newest `updated` already
in event_current. event_current is written LAST, so if this job dies after
history but before current, the next run re-reads the same versions -- and the
history MERGE is keyed on (event_id, valid_from), so they do not duplicate.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    CHANGELOG,
    CURRENT,
    HISTORY,
    NS_BRONZE,
    NS_SILVER,
    TRACKED,
    build_spark,
    check_access,
)

DATA_COLS = ["event_time", "mag", "mag_type", "place", "longitude", "latitude",
             "depth_km", "status", "alert", "tsunami", "sig", "net", "event_type"]


def quality(df):
    """Drop rows that cannot be modelled, and COUNT each reason."""
    rules = [
        ("event_time missing", F.col("event_time").isNotNull()),
        ("magnitude out of range", F.col("mag").isNull() | F.col("mag").between(-2, 10)),
        ("latitude out of range", F.col("latitude").isNull() | F.col("latitude").between(-90, 90)),
        ("longitude out of range", F.col("longitude").isNull() | F.col("longitude").between(-180, 180)),
    ]
    total = df.count()
    for name, keep in rules:
        n = df.where(~keep).count()
        if n:
            print(f"      dropped {n:,} rows: {name}")
        df = df.where(keep)
    kept = df.count()
    print(f"      quality: kept {kept:,} of {total:,}")
    return df


def main() -> None:
    spark = build_spark("earthquakes-silver")

    check_access(spark, namespaces=[NS_BRONZE, NS_SILVER],
                 read=[CHANGELOG], read_if_exists=[HISTORY, CURRENT])

    hwm = None
    if spark.catalog.tableExists(CURRENT):
        hwm = spark.table(CURRENT).agg(F.max("updated")).first()[0]
    print(f"[1/4] New versions since {hwm or 'the beginning'}")

    new = spark.table(CHANGELOG)
    if hwm is not None:
        new = new.where(F.col("updated") > F.lit(hwm))
    new = new.select("event_id", "updated", *DATA_COLS).dropDuplicates(["event_id", "updated"])
    new = quality(new)
    if new.limit(1).count() == 0:
        print("      nothing new; done")
        spark.stop()
        return

    hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in TRACKED]), 256)
    new = new.withColumn("hash_diff", hash_expr).cache()

    # ---------------------------------------------------------------- SCD2 --
    print(f"[2/4] SCD Type 2 -> {HISTORY}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HISTORY} (
            event_id STRING, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN,
            event_time TIMESTAMP, mag DOUBLE, mag_type STRING, place STRING,
            longitude DOUBLE, latitude DOUBLE, depth_km DOUBLE,
            status STRING, alert STRING, tsunami INT, sig INT, net STRING, event_type STRING,
            hash_diff STRING
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)
    cols = ["event_id", "valid_from", *DATA_COLS, "hash_diff"]

    ids = new.select("event_id").distinct()
    cur = (spark.table(HISTORY).where("is_current").join(ids, "event_id")
           .select(*cols).withColumn("_existing", F.lit(True)))

    # Only versions NEWER than what history already holds for that event. On a
    # re-run after a partial failure this is what keeps an already-inserted
    # version from being staged a second time.
    cand = (new.withColumnRenamed("updated", "valid_from")
            .join(cur.select("event_id", F.col("valid_from").alias("_cur_from")), "event_id", "left")
            .where(F.col("_cur_from").isNull() | (F.col("valid_from") > F.col("_cur_from")))
            .select(*cols).withColumn("_existing", F.lit(False)))

    w = Window.partitionBy("event_id").orderBy("valid_from")
    staged = (
        cur.unionByName(cand)
        .withColumn("_prev_hash", F.lag("hash_diff").over(w))
        # A version whose tracked columns equal the one before it is not a new
        # state -- only `updated` moved. Keep the existing row, drop the repeat.
        .where(F.col("_existing") | F.col("_prev_hash").isNull() | (F.col("hash_diff") != F.col("_prev_hash")))
        .withColumn("valid_to", F.lead("valid_from").over(w))
        .withColumn("is_current", F.col("valid_to").isNull())
        # Existing rows only need writing if a newer state just closed them.
        .where(~F.col("_existing") | F.col("valid_to").isNotNull())
        .drop("_prev_hash", "_existing")
    )
    opened = staged.where("is_current").count()
    closed = staged.where("NOT is_current").count()
    staged.createOrReplaceTempView("scd2_staged")
    spark.sql(f"""
        MERGE INTO {HISTORY} t
        USING scd2_staged s
        ON t.event_id = s.event_id AND t.valid_from = s.valid_from
        WHEN MATCHED THEN UPDATE SET t.valid_to = s.valid_to, t.is_current = s.is_current
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"      rows written: {opened:,} current, {closed:,} closed or historical")

    # ---------------------------------------------------------------- SCD1 --
    print(f"[3/4] SCD Type 1 -> {CURRENT}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CURRENT} (
            event_id STRING, updated TIMESTAMP,
            event_time TIMESTAMP, mag DOUBLE, mag_type STRING, place STRING,
            longitude DOUBLE, latitude DOUBLE, depth_km DOUBLE,
            status STRING, alert STRING, tsunami INT, sig INT, net STRING, event_type STRING
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)
    latest = (new.withColumn("_rn", F.row_number().over(
                  Window.partitionBy("event_id").orderBy(F.col("updated").desc())))
              .where("_rn = 1").select("event_id", "updated", *DATA_COLS))
    latest.createOrReplaceTempView("scd1_latest")
    spark.sql(f"""
        MERGE INTO {CURRENT} t
        USING scd1_latest s
        ON t.event_id = s.event_id
        WHEN MATCHED AND s.updated > t.updated THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("[4/4] Totals")
    print(f"      {CURRENT}: {spark.table(CURRENT).count():,} events")
    print(f"      {HISTORY}: {spark.table(HISTORY).count():,} states")
    spark.stop()


if __name__ == "__main__":
    main()
