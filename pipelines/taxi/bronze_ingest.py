"""
bronze: land ONE DAY of NYC TLC Yellow Taxi trips, exactly as published.

    spark-submit bronze_ingest.py --date 2024-01-15

WHAT BRONZE IS FOR
-------------------
A faithful copy of the source plus enough metadata to answer "where did this
row come from and when did we get it". No renaming, no type coercion, no
quality rules. Every cleaning decision belongs in silver, where it can be
changed and re-run without going back to the publisher -- who may have replaced
the file by then.

The one thing that IS selected here is the day. Choosing which increment to
load is not cleaning; it is what makes the pipeline incremental.

A MONTHLY SOURCE, A DAILY PIPELINE
------------------------------------
TLC publishes one Parquet file per month. This job downloads that file and
keeps only the rows whose pickup falls on the requested day, so ~60 MB crosses
the NAT gateway to produce ~1/30th of it.

That is deliberate, and the alternative is worse. Landing whole months would
make every downstream task month-grained too: a bad row anywhere in January
would mean re-running all of January, and the dashboard could not be refreshed
for one day without rebuilding thirty. Roughly $0.003 of NAT traffic per run
buys a pipeline whose unit of failure and unit of repair are both one day.

If the volume ever justifies it, the fix is to cache the month's file in S3 and
have the daily runs read from there -- one download per month, still one day
per run. That is a change to this file only.

WHY THE DRIVER STREAMS THE FILE
---------------------------------
Spark cannot read https:// directly, so the bytes pass through the driver.
Reading a whole month into memory would need ~4 GB of heap; the row-group loop
below holds one batch at a time and filters it before Spark ever sees it, which
keeps the driver inside 2 GB regardless of how busy the month was.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import tempfile

import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    BRONZE,
    PARTITION_COL,
    TLC_BASE,
    build_spark,
    date_arg,
    fail,
    replace_day,
    source_month,
)

BATCH_ROWS = 400_000

# The column TLC uses for pickup time. Named once: the schema has changed name
# across years, and a rename would otherwise have to be found in three places.
PICKUP_COL = "tpep_pickup_datetime"


def download(month: str, dest: str) -> int:
    url = f"{TLC_BASE}/trip-data/yellow_tripdata_{month}.parquet"
    print(f"[1/4] Downloading {url}")
    try:
        # stream=True so a 60 MB file is never held in memory as one bytes
        # object on top of the copy being written to disk.
        with requests.get(url, stream=True, timeout=(10, 300)) as r:
            if r.status_code in (403, 404):
                fail(
                    f"TLC has no file for {month} (HTTP {r.status_code}). They "
                    f"publish with roughly a two-month lag, so recent months do "
                    f"not exist yet."
                )
            r.raise_for_status()
            size = 0
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
                    size += len(chunk)
    except requests.RequestException as exc:
        fail(f"download failed: {exc}")
    print(f"      {size / 1e6:.1f} MB")
    return size


def main() -> None:
    day = date_arg(sys.argv)
    month = source_month(day)
    run_id = os.environ.get("SPARK_APPLICATION_NAME", "manual")
    source_file = f"yellow_tripdata_{month}.parquet"

    # Half-open interval [day, day+1). Written this way rather than as a date
    # cast so the comparison stays on the native timestamp column -- casting
    # every row to a date first would be a full scan of the batch for nothing.
    lo = dt.datetime.combine(day, dt.time.min)
    hi = lo + dt.timedelta(days=1)

    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, source_file)
        download(month, local)

        spark = build_spark(f"taxi-bronze-{day.isoformat()}")

        print(f"[2/4] Making {day} re-runnable...")
        replace_day(spark, BRONZE, day)

        print(f"[3/4] Filtering to {day} and writing...")
        pf = pq.ParquetFile(local)
        if PICKUP_COL not in pf.schema_arrow.names:
            fail(
                f"the source file has no column {PICKUP_COL!r}. TLC changed the "
                f"schema; columns are: {pf.schema_arrow.names}"
            )

        ingested_at = dt.datetime.now(dt.timezone.utc)
        scanned = 0
        kept = 0
        first = not spark.catalog.tableExists(BRONZE)

        for i, batch in enumerate(pf.iter_batches(batch_size=BATCH_ROWS)):
            scanned += batch.num_rows

            # Filter in Arrow, before Spark. A day is ~3% of a month, so this
            # is the difference between shipping 3,000,000 rows through py4j
            # and shipping 100,000.
            mask = pc.and_(
                pc.greater_equal(batch.column(PICKUP_COL), pc.scalar(lo)),
                pc.less(batch.column(PICKUP_COL), pc.scalar(hi)),
            )
            batch = batch.filter(mask)
            if batch.num_rows == 0:
                continue

            df = spark.createDataFrame(batch.to_pandas())

            # The metadata that makes bronze auditable. trip_date is set from
            # the ARGUMENT, not derived from the data -- the filter above has
            # already guaranteed they agree, and a literal cannot produce a
            # partition for the year 2098 out of one corrupt timestamp.
            df = (
                df.withColumn(PARTITION_COL, F.lit(day.isoformat()).cast("date"))
                .withColumn("_ingested_at", F.lit(ingested_at))
                .withColumn("_source_file", F.lit(source_file))
                .withColumn("_run_id", F.lit(run_id))
            )

            if first:
                # createOrReplace on the first written batch only; append after.
                # Doing it per batch would leave the table holding just the last.
                df.writeTo(BRONZE).partitionedBy(PARTITION_COL).createOrReplace()
                first = False
            else:
                df.writeTo(BRONZE).append()

            kept += batch.num_rows
            print(f"      batch {i}: kept {batch.num_rows:,} (running total {kept:,})")

    print("[4/4] Verifying what landed...")
    if kept == 0:
        fail(
            f"no trips on {day} in {source_file}. Either the date is outside the "
            f"month the file covers, or TLC published an empty file."
        )

    written = spark.table(BRONZE).where(F.col(PARTITION_COL) == day.isoformat()).count()
    if written != kept:
        fail(f"wrote {kept:,} rows but the table reports {written:,} for {day}")
    pct = kept / scanned * 100 if scanned else 0
    print(f"      {written:,} rows in {BRONZE} for {day}")
    print(f"      ({pct:.1f}% of the {scanned:,} rows in the monthly file)")
    spark.stop()


if __name__ == "__main__":
    main()
