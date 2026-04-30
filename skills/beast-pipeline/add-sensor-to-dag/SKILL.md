---
name: beast-pipeline-add-sensor-to-dag
description: Wire an existing sensor class into a BEAST DAG — S3 _SUCCESS marker via S3SuccessSensorFactory, Redshift freshness via RedshiftFreshnessSensor, or cross-DAG wait via ExternalTaskSensor. Use when adding a new upstream dependency to a DAG, adjusting timeouts and poke intervals, or choosing between the three sensor types. For building a NEW sensor class from scratch or modifying existing sensor behavior, use beast-pipeline-create-custom-sensor instead.
---

# Add a Sensor to a BEAST DAG

Airflow on MWAA `mw1.micro` is an **executor-constrained environment**
(max 25 active DAGs, 32 parallel tasks cluster-wide). Sensors are the
biggest source of executor pressure, so pick the right one and configure
it correctly.

## Decision table

| Signal you need to wait on | Use | Section |
|---|---|---|
| An S3 partition marked with a `_SUCCESS` or `*.parquet` file | `S3SuccessSensorFactory` → `S3SuccessCustomSensor` | [S3 section](#s3-sensors) |
| A Redshift table row count for a specific partition / condition | `RedshiftFreshnessSensor` | [Redshift section](#redshift-sensors) |
| Another DAG (or a specific task) to finish | `ExternalTaskSensor` | [Cross-DAG section](#cross-dag-sensors) |
| An API / endpoint response | Stop — come to MLOps, this is rare enough that we'd want to discuss pattern before adding a new sensor class | n/a |

Full sensor catalog:
[`docs/sensors.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/sensors.md).

## Universal rules

1. **Always `mode="reschedule"`**, never `"poke"`. Poke mode holds an
   executor slot for the full wait; reschedule mode releases it between
   checks. On `mw1.micro` this matters a lot.
2. **`poke_interval`** — never less than 5 minutes (`60 * 5`) for
   fast-moving checks; one hour (`60 * 60`) for most feature partitions.
3. **`timeout`** — pick from the canonical tiers:
   - 72 hours (`60 * 60 * 72`) — same-cycle features.
   - 168 hours (`60 * 60 * 168`) — monthly / cross-cycle features where
     the upstream producer has a wider SLO.
4. **Never inline sensor logic**. Use the factory (S3) or the provided
   class (Redshift). Custom `PythonSensor` subclasses need a design
   discussion first.

## S3 sensors

Use [`S3SuccessSensorFactory`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/sensor_factory.py).
It builds a list of `S3SuccessCustomSensor` instances from config dicts,
de-duplicates `task_id`s, and applies shared defaults.

### Pattern

```python
from sensors.sensor_factory import S3SuccessSensorFactory

factory = S3SuccessSensorFactory(
    common_config={
        "aws_conn_id": AWS_CONN_ID,
        "poke_interval": 60 * 60,
    }
)

sensor_configs = [
    {
        "task_id": "wait_s3_partition_completeness_my_feature",
        "bucket_name": S3_ANALYTICAL_BUCKET,
        "prefix": f"feature-platform/my-feature/dt={derived_cycle_end_date}/",
        "timeout": 60 * 60 * 72,
    },
    {
        "task_id": "wait_s3_partition_completeness_buro_features",
        "bucket_name": S3_DE_MODEL_FEATURES_BUCKET,
        "prefix": f"prod/ccm/mx-buro-features/features_v3/account/dt={first_day_of_month}/",
        "success_suffix": "*.parquet",  # override when upstream emits parquet, not _SUCCESS
        "timeout": 60 * 60 * 168,
    },
]

s3_sensors = factory.create_sensors_list(sensor_configs)
```

Then wire into the task graph:

```python
derived_cycle_end_date >> check_s3_partition_task >> [*s3_sensors] >> trigger_sagemaker
```

### `success_suffix`

- Default: `_SUCCESS` (Hadoop/Spark standard).
- Override to `*.parquet` when the upstream producer doesn't emit a
  `_SUCCESS` file but lands parquet directly (common for buro features,
  IMSS drops).
- Override to any other literal (e.g. `_DONE`) for custom upstream
  producers.

### `fallback_prefix`

Use when the upstream partition could land in either the current month
or the previous month (e.g., IMSS data that's released monthly with
irregular timing). The sensor checks the primary prefix first, then the
fallback; succeeds if either has a match.

```python
{
    "task_id": "wait_s3_partition_completeness_imss",
    "bucket_name": S3_ANALYTICAL_BUCKET,
    "prefix": f"feature-platform/imss-raw/monthly-bau/mth={year_month}/",
    "fallback_prefix": f"feature-platform/imss-raw/monthly-bau/mth={prev_year_month}/",
    "success_suffix": "*.parquet",
    "timeout": 60 * 60 * 168,
},
```

See `ccm_score_v5.py` for the real-world example.

### Don't use the factory if…

- You only need **one** sensor — instantiate `S3SuccessCustomSensor`
  directly. The factory is overkill for a single check.
- You need a sensor with fundamentally different behavior. Don't extend
  the factory — raise it in a design discussion.

## Redshift sensors

Use [`RedshiftFreshnessSensor`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/redshift_freshness_sensor.py).
It's a thin `SqlSensor` subclass that checks whether a Redshift table has
rows matching a given condition, grouped by date.

### Pattern

```python
from sensors.redshift_freshness_sensor import RedshiftFreshnessSensor

wait_master_cc_stmt_ledger = RedshiftFreshnessSensor(
    task_id="wait_master_cc_stmt_ledger",
    conn_id=REDSHIFT_CONN_ID,                              # ds_mlops / ds_feature / ds_collections
    table="stori_ccm_pl.master_cc_stmt_ledger_v3",         # fully-qualified
    column="cycle_end_dt",                                 # date-typed column
    condition=f"cycle_end_dt = '{derived_cycle_end_date}'", # SQL WHERE fragment
    poke_interval=60 * 30,
    timeout=60 * 60 * 72,
    mode="reschedule",
)
```

### Connection id — use the right one

| Conn id | When |
|---|---|
| `ds_mlops` | Scoring DAGs reading/writing the MLOps schema. |
| `ds_feature` | Feature DAGs waiting on Feature team tables. |
| `ds_collections` | Anything in the Collections schema. |

Full list:
[`docs/connections-and-variables.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/connections-and-variables.md).

### SQL that the sensor runs

```sql
SELECT num
FROM (
    SELECT DATE(<column>) as dt, count(*) as num
    FROM <table>
    WHERE <condition>
    GROUP BY 1
    ORDER BY 1 desc
    LIMIT 1
)
```

The sensor succeeds if the query returns any row. That means: rows
exist matching the condition. It does **not** check the exact count —
just the existence. If you need a minimum row count, compose a manual
`SqlSensor` with a custom `success` predicate (rare — raise with MLOps
first).

## Cross-DAG sensors

Use `airflow.sensors.external_task.ExternalTaskSensor` to make a
downstream DAG wait on an upstream DAG's run (or a specific task within
it).

### Pattern — monitoring waits on scoring

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_ccm_score_v5 = ExternalTaskSensor(
    task_id="wait_for_ccm_score_v5_dag",
    external_dag_id="ccm_score_v5",
    external_task_id="refresh_cubo_mv",
    # Allow "skipped" so monitoring still runs if scoring short-circuited.
    allowed_states=["success", "skipped"],
    check_existence=True,
    poke_interval=60 * 5,
    timeout=60 * 60 * 168,
    mode="reschedule",
)
```

### Pattern — scoring waits on shared sensors DAG with offset schedules

When the two DAGs run at **different times of day**, you must map the
logical_date to make Airflow find the right upstream run. This is the
most common footgun in cross-DAG wiring.

```python
wait_for_common_sensors = ExternalTaskSensor(
    task_id="wait_for_common_sensors_dag",
    external_dag_id="ccm_score_common_sensors",
    external_task_id=None,  # wait for the entire DAG
    allowed_states=["success"],
    check_existence=True,
    poke_interval=60 * 5,
    timeout=60 * 60 * 168,
    mode="reschedule",
    execution_date_fn=lambda dt: dt.replace(hour=0, minute=0, second=0, microsecond=0),
)
```

Here the downstream DAG runs at `14:00` but the shared-sensors DAG runs
at `00:00`. Without `execution_date_fn`, Airflow would look for a
`14:00` run of `ccm_score_common_sensors` — which never exists.

### Pattern — choose `external_task_id` carefully

| `external_task_id` value | Meaning |
|---|---|
| A specific task id | Wait on exactly that task. Use when you need to gate on a specific step. |
| `None` | Wait on the **entire DAG** to reach a final state. Use for full-pipeline dependencies. |

### Allowed states cheat sheet

- **`["success"]`** — strict. The upstream must succeed.
- **`["success", "skipped"]`** — relaxed. Use when the upstream DAG has
  a short-circuit that may skip its final task (e.g., scoring DAG skips
  its MV refresh when the output partition already exists).

## Common mistakes

- **Hard-coded bucket name** — never. Always
  `f"cca-ds-analytical-{ENVIRONMENT}"` or the right `{ENVIRONMENT}`-templated
  bucket.
- **Hard-coded partition date** — always reference the `derived_cycle_end_date`
  XCom, not a date literal.
- **Forgetting the `/` suffix** — the factory adds it for you, but if
  you instantiate `S3SuccessCustomSensor` directly, ensure the prefix
  ends with `/`.
- **`poke_interval=0`** — Airflow rejects this; minimum is a few seconds
  but practical minimum is 60 seconds. Our convention is 5 minutes or 1
  hour.
- **Duplicate `task_id`s** — the factory raises; Airflow would too. All
  task ids must be unique per DAG.

## Next steps

With sensors wired, return to [create-dag](../create-dag/SKILL.md) to
finish the DAG, then follow
[`docs/runbooks/onboarding-a-new-dag.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/runbooks/onboarding-a-new-dag.md)
to register ownership, write tests, and open the PR.
