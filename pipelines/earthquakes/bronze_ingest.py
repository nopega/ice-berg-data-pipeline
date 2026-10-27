"""
bronze: incremental, query-based CDC from the USGS earthquake API.

    spark-submit bronze_ingest.py                      # from the watermark
    spark-submit bronze_ingest.py --since 2026-10-01   # re-read from a point in time

WHAT THIS JOB DOES
------------------
0. Governance check: prove this run's principal CANNOT read another pipeline's
   table. If Polaris lets it, the access model is broken and nothing runs.
1. Watermark: the newest `updated` already in the changelog. Derived from the
   table itself, so it only moves when a write has committed -- a run that
   fails half-way leaves the watermark where it was and the next run re-reads
   the same window.
2. Pull every event USGS has changed since the watermark (`updatedafter`),
   paging through the 20,000-per-request limit.
3. MERGE into the changelog on (event_id, updated): a version already landed is
   skipped, a new one is appended. Re-running the same window is a no-op.

WHY "QUERY-BASED" CDC
---------------------
There is no database log to read; the source is an HTTP API. Each event's
`updated` timestamp is the change marker, and every distinct value of it is a
version. This captures inserts and updates. It CANNOT capture deletes: an event
USGS removes simply stops appearing. That is a property of pulling from an API,
and it is stated here rather than discovered later.
"""

from __future__ import annotations

import datetime as dt
import sys
import os

import requests
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (  # noqa: E402
    CHANGELOG,
    FORBIDDEN_PROBE,
    build_spark,
    fail,
    opt_arg,
    parse_utc,
    run_id,
)

API = "https://earthquake.usgs.gov/fdsnws/event/1/query"
PAGE = 20_000                 # the service's hard maximum per request
INITIAL_DAYS = 3              # how far back the very first run reads
OVERLAP = dt.timedelta(hours=1)  # re-read a little before the watermark; MERGE dedupes
# `updatedafter` filters on change time, but the service ALSO applies its
# default event-time window (the last 30 days) unless starttime is given. An
# event older than this that USGS revises is therefore not seen -- an accepted
# limit, named here.
EVENT_WINDOW = dt.timedelta(days=30)

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("updated", T.TimestampType(), False),
    T.StructField("event_time", T.TimestampType(), True),
    T.StructField("mag", T.DoubleType(), True),
    T.StructField("mag_type", T.StringType(), True),
    T.StructField("place", T.StringType(), True),
    T.StructField("longitude", T.DoubleType(), True),
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("depth_km", T.DoubleType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("alert", T.StringType(), True),
    T.StructField("tsunami", T.IntegerType(), True),
    T.StructField("sig", T.IntegerType(), True),
    T.StructField("net", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
])


def governance_check(spark) -> None:
    """Fail the run if this principal can read a table outside its own domain."""
    print(f"[0/3] Governance check: must NOT be able to read {FORBIDDEN_PROBE}")
    try:
        spark.table(FORBIDDEN_PROBE).limit(0).collect()
    except Exception as exc:  # noqa: BLE001 -- classify by message below
        msg = str(exc)
        if any(k in msg for k in ("Forbidden", "NotAuthorized", "not authorized", "403")):
            print("      denied by Polaris, as it should be")
            return
        if "NoSuchTable" in msg or "TABLE_OR_VIEW_NOT_FOUND" in msg or "NoSuchNamespace" in msg:
            print("      WARNING: probe table does not exist, so the check proves nothing")
            return
        fail(f"governance check: unexpected error reading the probe table: {msg[:500]}")
    fail(
        f"governance check: this principal CAN read {FORBIDDEN_PROBE}. "
        f"The pipeline is running with broader access than its own domain -- "
        f"check which credential the SparkApplication mounted."
    )


def watermark(spark) -> dt.datetime | None:
    if not spark.catalog.tableExists(CHANGELOG):
        return None
    # As epoch seconds, not a Python datetime: collect() turns a TIMESTAMP into
    # a naive datetime in the PROCESS's local zone, which is an easy hour (or
    # seven) to lose if the pod ever runs with TZ set.
    v = spark.table(CHANGELOG).agg(F.max(F.unix_micros("updated"))).first()[0]
    return dt.datetime.fromtimestamp(v / 1e6, tz=dt.timezone.utc) if v is not None else None


def ms(v):
    """USGS epoch milliseconds -> AWARE UTC datetime (Spark converts it exactly)."""
    return dt.datetime.fromtimestamp(v / 1000, tz=dt.timezone.utc) if v is not None else None


def to_row(f: dict):
    p = f.get("properties") or {}
    c = (f.get("geometry") or {}).get("coordinates") or [None, None, None]
    c = (list(c) + [None, None, None])[:3]
    num = lambda x: float(x) if x is not None else None  # noqa: E731
    return (
        f["id"], ms(p.get("updated")), ms(p.get("time")),
        num(p.get("mag")), p.get("magType"), p.get("place"),
        num(c[0]), num(c[1]), num(c[2]),
        p.get("status"), p.get("alert"),
        int(p["tsunami"]) if p.get("tsunami") is not None else None,
        int(p["sig"]) if p.get("sig") is not None else None,
        p.get("net"), p.get("type"),
    )


def fetch(since: dt.datetime) -> list:
    params = {
        "format": "geojson",
        "updatedafter": since.strftime("%Y-%m-%dT%H:%M:%S"),
        "starttime": (since - EVENT_WINDOW).strftime("%Y-%m-%dT%H:%M:%S"),
        "orderby": "time-asc",
        "limit": PAGE,
    }
    rows, offset = [], 1
    while True:
        params["offset"] = offset
        try:
            r = requests.get(API, params=params, timeout=(10, 120))
        except requests.RequestException as exc:
            fail(f"USGS request failed: {exc}")
        if r.status_code != 200:
            fail(f"USGS answered HTTP {r.status_code}: {r.text[:300]}")
        feats = r.json().get("features", [])
        rows += [to_row(f) for f in feats if f.get("id") and (f.get("properties") or {}).get("updated")]
        print(f"      page at offset {offset}: {len(feats):,} events")
        if len(feats) < PAGE:
            return rows
        offset += PAGE


def main() -> None:
    spark = build_spark("earthquakes-bronze")
    governance_check(spark)

    override = opt_arg(sys.argv, "--since")
    wm = watermark(spark)
    if override:
        since = parse_utc(override)
        how = "--since override"
    elif wm:
        since = wm - OVERLAP
        how = f"watermark {wm.isoformat()} minus {OVERLAP}"
    else:
        since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=INITIAL_DAYS)
        how = f"first run: last {INITIAL_DAYS} days"
    print(f"[1/3] Reading changes since {since.isoformat()} ({how})")

    print("[2/3] Fetching from USGS...")
    rows = fetch(since)
    if not rows:
        print("      nothing changed since the watermark; done")
        spark.stop()
        return

    df = (
        spark.createDataFrame(rows, SCHEMA)
        .dropDuplicates(["event_id", "updated"])
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id()))
        .withColumn("_source", F.lit(API))
    )

    print(f"[3/3] Merging {df.count():,} versions into {CHANGELOG}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CHANGELOG} (
            event_id STRING, updated TIMESTAMP, event_time TIMESTAMP,
            mag DOUBLE, mag_type STRING, place STRING,
            longitude DOUBLE, latitude DOUBLE, depth_km DOUBLE,
            status STRING, alert STRING, tsunami INT, sig INT, net STRING, event_type STRING,
            _ingested_at TIMESTAMP, _run_id STRING, _source STRING
        ) USING iceberg
        PARTITIONED BY (days(_ingested_at))
    """)
    before = spark.table(CHANGELOG).count()
    df.createOrReplaceTempView("incoming")
    # Insert-only MERGE: a version (event_id, updated) is immutable once landed.
    spark.sql(f"""
        MERGE INTO {CHANGELOG} t
        USING incoming s
        ON t.event_id = s.event_id AND t.updated = s.updated
        WHEN NOT MATCHED THEN INSERT *
    """)
    after = spark.table(CHANGELOG).count()
    print(f"      new versions: {after - before:,} (already present: {df.count() - (after - before):,})")
    print(f"      changelog now holds {after:,} versions")
    spark.stop()


if __name__ == "__main__":
    main()
