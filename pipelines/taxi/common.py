"""
Shared setup for every task in the NYC taxi pipeline.

Downloaded by Spark at submit time, from raw.githubusercontent.com, because the
SparkApplication lists it in deps.pyFiles. It is NOT baked into the image.

That is why it imports nothing outside PySpark and the standard library:
whatever is used here has to already exist in
harbor.nopega.net/ice-berg-platform/datapipeline.

Adding another shared module means adding it to deps.pyFiles in all three
templates -- Spark downloads exactly the files it is told about.
"""

from __future__ import annotations

import datetime as dt
import os
import sys

from pyspark.sql import SparkSession

CATALOG = "data_platform"

BRONZE = f"{CATALOG}.bronze.transactional.taxi_trip"
SILVER = f"{CATALOG}.silver.derived.taxi_trip_cleaned"
GOLD = f"{CATALOG}.gold.aggregate.taxi_daily_zone_revenue"

# No backticks around the namespace. Spark's multipart identifiers already
# express nesting; quoting the dotted string asks Polaris for a single
# namespace whose name contains dots, and it correctly answers
# NoSuchNamespaceException. This cost a debugging cycle during the smoke test.

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"

# Every table is partitioned on this one column, and every task writes exactly
# one value of it. That is what makes a task re-runnable: it deletes its own
# partition before writing, so a retry after a partial write leaves the same
# table as a clean first run.
PARTITION_COL = "trip_date"


def fail(msg: str) -> None:
    """Exit non-zero with a message the Airflow log will show at the end."""
    print(f"\nTASK FAILED: {msg}\n", file=sys.stderr)
    sys.exit(1)


def require_env(*names: str) -> None:
    """
    Fail before touching anything if configuration is missing.

    The alternative is a job that starts, spends four minutes acquiring
    executors, and then dies on a KeyError -- which reads like a code bug
    rather than a missing Secret.
    """
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        fail("required environment variables are not set: " + ", ".join(missing))


def build_spark(app_name: str) -> SparkSession:
    """
    A SparkSession wired to Polaris.

    Every line here is load-bearing:
      type=rest        -> speak the Iceberg REST protocol to Polaris
      credential       -> the `etl` principal; Polaris decides what S3
                          credentials to vend from ITS privileges, so this is
                          where the platform's access control actually binds
      io-impl=S3FileIO -> Iceberg's own S3 client rather than Hadoop S3A
    """
    require_env("POLARIS_URI", "POLARIS_CREDENTIAL", "WAREHOUSE_BUCKET")

    spark = (
        SparkSession.builder.appName(app_name)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.environ["POLARIS_URI"])
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", CATALOG)
        .config(f"spark.sql.catalog.{CATALOG}.credential", os.environ["POLARIS_CREDENTIAL"])
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Arrow makes the driver-side handoff of Pandas batches into Spark
        # roughly an order of magnitude faster. Without it the bronze task
        # spends most of its time serialising rows one at a time through py4j.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def date_arg(argv: list[str]) -> dt.date:
    """
    Read --date YYYY-MM-DD, and reject anything that is not a real date.

    Parsed rather than pattern-matched, so "2024-02-30" is rejected here
    instead of silently producing a partition that can never contain a row.
    """
    raw = None
    for i, a in enumerate(argv):
        if a == "--date" and i + 1 < len(argv):
            raw = argv[i + 1]
    if not raw:
        fail("--date YYYY-MM-DD is required")
    try:
        return dt.date.fromisoformat(raw)
    except ValueError:
        fail(f"--date must be a real date in YYYY-MM-DD form, got {raw!r}")


def source_month(day: dt.date) -> str:
    """
    The TLC file that contains this day.

    The source is published one file per MONTH; the pipeline runs per DAY. This
    is the only place that mismatch is expressed, so it is the only place to
    change if TLC ever starts publishing daily.
    """
    return day.strftime("%Y-%m")


def replace_day(spark: SparkSession, table: str, day: dt.date) -> None:
    """
    Make a task re-runnable by deleting the day it is about to write.

    Airflow retries tasks. An append-only task that is retried after writing
    half its rows leaves duplicates that no error ever mentions -- they surface
    as a revenue figure that is 1.4x too high. Deleting first makes the whole
    task idempotent: running it twice leaves the same table as running it once.

    DELETE on an Iceberg table is a metadata operation plus a rewrite of only
    the affected files, not a full-table scan, because every table here is
    partitioned on this same column.
    """
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE {PARTITION_COL} = DATE '{day.isoformat()}'")
        print(f"      cleared existing rows for {day}")
