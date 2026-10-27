"""
Shared setup for the USGS earthquakes pipeline.

Downloaded by Spark at submit time (deps.pyFiles in every template), not baked
into the image -- so it imports nothing beyond PySpark, requests and the
standard library, all of which the datapipeline image already carries.

LAYOUT: medallion / category / domain / table
-----------------------------------------------
Every table of this pipeline lives under a namespace whose last level is the
domain, `earthquakes`. That level is what access control binds to: Polaris
principal `pl_earthquakes` is granted privileges on exactly these three
namespaces and nothing else (bootstrap/pipeline_principal.sh in
VPN_DATA_PLATFORM). A table created anywhere else cannot be written by this
pipeline at all.
"""

from __future__ import annotations

import datetime as dt
import os
import sys

from pyspark.sql import SparkSession

CATALOG = "data_platform"
DOMAIN = "earthquakes"

NS_BRONZE = f"bronze.transactional.{DOMAIN}"
NS_SILVER = f"silver.derived.{DOMAIN}"
NS_GOLD = f"gold.aggregate.{DOMAIN}"

# bronze: every version of every event the API ever returned (append-only CDC log)
CHANGELOG = f"{CATALOG}.{NS_BRONZE}.event_changelog"
# silver, SCD Type 1: one row per event, the latest version, overwritten in place
CURRENT = f"{CATALOG}.{NS_SILVER}.event_current"
# silver, SCD Type 2: one row per DISTINCT state of an event, with validity range
HISTORY = f"{CATALOG}.{NS_SILVER}.event_history"
# gold: per event day and region
DAILY = f"{CATALOG}.{NS_GOLD}.daily_summary"

# Columns whose change makes a new SCD Type 2 version. `updated` is NOT among
# them: USGS bumps it for edits we do not model (a new contributing network, a
# product attachment), and each of those would otherwise open a version that is
# identical to the last one in every column anyone reads.
TRACKED = ["mag", "mag_type", "place", "latitude", "longitude", "depth_km",
           "status", "alert", "tsunami", "event_type"]


def fail(msg: str) -> None:
    """Exit non-zero with a message the Airflow log shows at the end."""
    print(f"\nTASK FAILED: {msg}\n", file=sys.stderr)
    sys.exit(1)


def require_env(*names: str) -> None:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        fail("required environment variables are not set: " + ", ".join(missing))


def build_spark(app_name: str) -> SparkSession:
    """
    A SparkSession wired to Polaris as principal pl_earthquakes.

    The credential comes from Secret spark/polaris-pl-earthquakes, NOT the
    shared `etl` one. Polaris vends S3 credentials from the asking principal's
    privileges, so this line is where "this pipeline sees only its own tables"
    is actually enforced.
    """
    require_env("POLARIS_URI", "POLARIS_CREDENTIAL")
    spark = (
        SparkSession.builder.appName(app_name)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.environ["POLARIS_URI"])
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", CATALOG)
        .config(f"spark.sql.catalog.{CATALOG}.credential", os.environ["POLARIS_CREDENTIAL"])
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # All timestamps here are UTC from the source; keep Spark from shifting
        # them into the pod's zone when it derives event dates.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_access(spark, namespaces=(), read=(), read_if_exists=()) -> None:
    """
    Step 0 of every job: prove this principal can reach exactly what the job is
    about to use, BEFORE any work starts.

        namespaces       must be listable (TABLE_LIST on the namespace)
        read             must exist and be readable (loads the table with
                         vended S3 credentials, i.e. TABLE_READ_DATA)
        read_if_exists   readable if present; absent is fine (first run)

    Without this, a missing grant shows up several minutes into a run as a
    Java stack trace from deep inside a MERGE. Here it is one line naming the
    table and the script that grants it.
    """
    who = "pl_earthquakes"
    print(f"[0] Access check as {who}")

    def denied(what: str, exc: Exception) -> None:
        msg = str(exc)
        hint = (" -- the grant is missing; re-run "
                "VPN_DATA_PLATFORM/bootstrap/pipeline_principal.sh earthquakes")
        if not any(k in msg for k in ("Forbidden", "NotAuthorized", "not authorized", "403")):
            hint = ""
        fail(f"access check: cannot {what}{hint}\n      {msg[:400]}")

    for ns in namespaces:
        try:
            spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").collect()
        except Exception as exc:  # noqa: BLE001
            denied(f"list namespace {ns}", exc)
        print(f"      list  {ns}: ok")

    for t in [*read, *read_if_exists]:
        if not spark.catalog.tableExists(t):
            if t in read:
                fail(f"access check: {t} does not exist -- has the upstream task run?")
            print(f"      read  {t}: not created yet (first run)")
            continue
        try:
            spark.table(t).limit(1).collect()
        except Exception as exc:  # noqa: BLE001
            denied(f"read {t}", exc)
        print(f"      read  {t}: ok")


def opt_arg(argv: list[str], name: str) -> str | None:
    """Value of `--name X`, or None. Empty strings count as absent."""
    for i, a in enumerate(argv):
        if a == name and i + 1 < len(argv) and argv[i + 1].strip():
            return argv[i + 1].strip()
    return None


def parse_utc(raw: str) -> dt.datetime:
    """ISO date or datetime -> aware UTC datetime. Rejects anything else."""
    try:
        v = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        fail(f"not an ISO date/datetime: {raw!r}")
    return v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)


def run_id() -> str:
    return os.environ.get("SPARK_APPLICATION_NAME", "manual")
