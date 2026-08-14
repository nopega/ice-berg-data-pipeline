# pipeline_repo — mirror of `github.com/nopega/ice-berg-data-pipeline`

The PySpark jobs the platform runs. **Nothing here is read from this
repository at runtime**: it mirrors a separate GitHub repo, which the Spark
driver fetches at pod start-up.

```
ice-berg-data-pipeline/         <- repo root
  pipelines/taxi/
    common.py                   catalog wiring, arg parsing, idempotent delete
    bronze_ingest.py
    silver_clean.py
    gold_aggregate.py
```

The `pipelines/` level is kept even though the repo is dedicated to pipelines,
so that the extraction path in the SparkApplication templates does not have to
change if a second family of jobs is ever added beside `taxi/`.

## Why this is not in the DAG repo

Airflow git-syncs `nopega/airflow_dag`. Whoever can push there can make the
scheduler run anything — that repository *is* production configuration, and it
should be gated accordingly.

Pipeline logic changes far more often, and is reviewed by people who need to
ship a fix to a filter without also holding the keys to the scheduler. Two
repos means two access lists.

**What it costs.** A change to `silver_clean.py` that `gold_aggregate.py`
depends on now spans two commits in two repos. Between the two pushes, a run
can pick up a `gold` that expects a column `silver` has not started writing.
For anything that must not move underneath itself, trigger with an exact
commit:

```json
{ "date": "2024-01-15", "pipeline_ref": "3f2a9c1" }
```

## How the code reaches a Spark pod

Not through Airflow. Spark itself resolves `https://` URIs, so the driver
downloads each file from `raw.githubusercontent.com` at submit time:

```yaml
mainApplicationFile: ".../pipelines/taxi/bronze_ingest.py"
deps:
  pyFiles:
    - ".../pipelines/taxi/common.py"
```

**A new shared module has to be added to `deps.pyFiles` in all three
templates.** Spark downloads exactly the files it is told about; a missing one
surfaces as `ModuleNotFoundError` after the pod has started and the executors
have been requested, not at submit time.

The image supplies the runtime — Spark 4.0.1, the Iceberg jars, the AWS SDK,
Python and its pinned libraries. This repo supplies the logic. Changing a
filter needs a push and a re-run; it needs no image rebuild and no Airflow
restart.

Consequences worth knowing:

- **The repo must be public.** A private one returns 404 and needs a token in
  the URL, which anything that can run `kubectl describe` would then be able to
  read.
- **Executors do not import this code today.** These jobs use only built-in
  Spark functions, so no Python closure is shipped to them. Spark distributes
  `pyFiles` to executors as well, so a UDF would still work — but the
  assumption is worth knowing.
- **`common.py` resolves by adjacency, not by PYTHONPATH.** Spark downloads
  `pyFiles` into the driver's working directory, which is also where the main
  file lands, and Python puts a script's own directory on `sys.path`. A bare
  `.py` on PYTHONPATH is not importable — `sys.path` entries must be
  directories or archives.

## Contract with the DAG

The DAG passes exactly one argument:

```
--date YYYY-MM-DD
```

Parsed with `date.fromisoformat`, not pattern-matched, so `2024-02-30` is
rejected at the top of the job rather than producing a partition that can never
contain a row.

Each job deletes that day from its target table, writes it, and verifies the
row count it wrote. Running a job twice leaves the same table as running it
once — which is what makes an Airflow retry safe.

Every table is partitioned on `trip_date`. The source is published per month;
only `source_month()` in `common.py` knows that, so it is the single place to
change if TLC ever starts publishing daily.
