---
name: create-dag
description: Create a new BEAST DAG — for a model (scoring/training), a feature engineering pipeline, or a monitoring workflow. Use when adding a new DAG to dags/beast_models/, dags/beast_features/, or dags/beast_model_monitoring/ in mlops-mwaa-dags. Emits a conventions-correct scaffold, wires the required on_failure_callback, short-circuit partition guard, sensors, and SageMakerStartPipelineOperator.
---

# Create a BEAST DAG

Use this sub-skill to onboard a new DAG into
[`mlops-mwaa-dags`](https://github.com/credifranco/mlops-mwaa-dags). It
covers all three DAG flavors: **model**, **feature**, and **monitoring**.

## Before you start — prerequisites

You need these from the model / feature owner before you write any DAG code:

1. **SageMaker pipeline name**, deployed in at least the target environment
   (`dev` and/or `prod`). DAGs here do not create pipelines; they trigger
   them.
2. **Upstream dependencies** — list the S3 prefixes, `_SUCCESS` / parquet
   markers, and Redshift freshness conditions the DAG must wait for.
3. **Output partition prefix** — used by the `@task.short_circuit`
   `check_if_partition_exists` guard so reruns are idempotent.
4. **Schedule** (cron) and **timezone expectation** (DAG default:
   `America/Mexico_City`).
5. **Owner** — must exist in
   [`dag_owners_config.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/utils/dag_owners_config.py).
   If not, register them first by adding an entry to `OWNER_SLACK_IDS`
   and `DAG_OWNERS` in the same PR.
6. **`pipeline_params`** expected by the SageMaker pipeline (names and
   sources — Airflow XCom vs. static).

If any of these are missing, stop and ask the user before generating code.

## Pick the flavor

| DAG flavor | Lives in | Canonical reference | Scaffold |
|---|---|---|---|
| **Model** (scoring or training) | `dags/beast_models/` | [`ccm_score_v5.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/beast_models/ccm_score_v5.py) | [`scripts/scaffold_model_dag.py`](scripts/scaffold_model_dag.py) |
| **Feature engineering** | `dags/beast_features/` | [`ccm_referral_dag.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/beast_features/ccm_referral_dag.py) | [`scripts/scaffold_feature_dag.py`](scripts/scaffold_feature_dag.py) |
| **Monitoring** | `dags/beast_model_monitoring/` | [`cubo_monitor_v5.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/beast_model_monitoring/cubo_monitor_v5.py) | [`scripts/scaffold_monitoring_dag.py`](scripts/scaffold_monitoring_dag.py) |

### When in doubt

- Model DAG runs **after** feature DAGs. It waits on feature partitions
  (via `S3SuccessSensorFactory`) + optional `ExternalTaskSensor` against
  a shared-sensors DAG, then triggers the scoring pipeline.
- Feature DAG runs earlier in the month. It waits on Redshift freshness
  (via `RedshiftFreshnessSensor`) for the upstream fact tables, then
  triggers the feature pipeline that lands the `_SUCCESS` marker the
  model DAG will sense.
- Monitoring DAG runs **after** the model DAG. It uses
  `ExternalTaskSensor` to wait on the scoring DAG, resolves the exact
  `score_creation_tms` partition, then triggers the monitoring pipeline.

## Quick start

Pick the flavor, run the scaffold, then edit the TODO markers:

```bash
# model DAG
python scripts/scaffold_model_dag.py \
    --dag-id my_model_score_v1 \
    --pipeline-base-name ds-my-model-v1-pipeline \
    --owner luis.rico \
    --schedule "0 14 13,28 * *" \
    --output dags/beast_models/my_model_score_v1.py

# feature DAG
python scripts/scaffold_feature_dag.py \
    --dag-id my_feature_v1_dag \
    --pipeline-name MyFeatureV1 \
    --owner erick.soriano \
    --redshift-conn ds_feature \
    --schedule "0 0 13,28 * *" \
    --output dags/beast_features/my_feature_v1_dag.py

# monitoring DAG
python scripts/scaffold_monitoring_dag.py \
    --dag-id my_model_monitor_v1 \
    --pipeline-base-name ds-my-model-monitoring-v1-pipeline \
    --upstream-score-dag my_model_score_v1 \
    --owner luis.rico \
    --schedule "0 14 13,28 * *" \
    --output dags/beast_model_monitoring/my_model_monitor_v1.py
```

Each scaffold emits a file that already:

- imports the right utilities,
- wires `DagFailureNotifier` + `dag_success_notification`,
- includes a `derive_cycle_end_date` task,
- includes a `@task.short_circuit check_if_partition_exists`,
- uses `S3SuccessSensorFactory` / `RedshiftFreshnessSensor` /
  `ExternalTaskSensor` as appropriate for the flavor, and
- calls `SageMakerStartPipelineOperator` with the mandated
  `display_name` and `wait_for_completion=True`.

TODO markers inside the scaffold point you at the values you still need to
fill in from the prerequisites above.

## Canonical patterns (read before editing any scaffold)

### 1. Constants block — always `{ENVIRONMENT}`-templated

```python
ENVIRONMENT = Variable.get("ENVIRONMENT")
AWS_CONN_ID = "aws_default"
SAGEMAKER_PIPELINE_NAME = f"ds-my-model-v1-pipeline-{ENVIRONMENT}"
S3_ANALYTICAL_BUCKET = f"cca-ds-analytical-{ENVIRONMENT}"
```

Never hard-code `dev` or `prod`.

### 2. `default_args` — failure alert is non-negotiable

```python
default_args = {
    "owner": "mlops",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 2, 27, tzinfo=pendulum.timezone("America/Mexico_City")),
    "on_failure_callback": [DagFailureNotifier(owner="luis.rico").get_callback()],
}
```

The `owner=` string must match a key in
[`dag_owners_config.OWNER_SLACK_IDS`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/utils/dag_owners_config.py).

### 3. `derive_cycle_end_date` — use the shared helper

Model and monitoring DAGs use the shared util. Feature DAGs sometimes
implement their own logic (see [`ccm_referral_dag.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/beast_features/ccm_referral_dag.py)
for the day-of-month branching pattern).

```python
from utils import derive_cycle_end_date_from_logical_date

@task
def derive_cycle_end_date(**kwargs) -> str:
    override = kwargs["params"].get("cycle_end_date_override", "")
    if override:
        return override
    logical_date = kwargs["data_interval_end"]
    return derive_cycle_end_date_from_logical_date(logical_date)
```

Always expose `cycle_end_date_override` in `params={}` so manual backfills
work without code edits.

### 4. `@task.short_circuit check_if_partition_exists` — always

```python
@task.short_circuit
def check_if_partition_exists(cycle_end_dt: str) -> bool:
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = f"MyModel/beast/outputs/cycle_end_date={cycle_end_dt}"
    exists = s3_hook.check_for_prefix(
        bucket_name=S3_ANALYTICAL_BUCKET, prefix=prefix, delimiter="/"
    )
    return not exists
```

This is what makes reruns safe. Without it, Airflow will re-trigger the
SageMaker pipeline on every catchup / manual run, and SageMaker will
happily re-do expensive work.

### 5. `SageMakerStartPipelineOperator` — mandated shape

```python
trigger_sagemaker = SageMakerStartPipelineOperator(
    task_id="trigger_sagemaker_score",
    pipeline_name=SAGEMAKER_PIPELINE_NAME,
    display_name=f"{SAGEMAKER_PIPELINE_NAME}-airflow-triggered-execution-{{{{ ts_nodash }}}}",
    wait_for_completion=True,
    check_interval=60 * 5,
    pipeline_params={
        # Always: push cycle_end_date / creation_timestamp via XCom, NOT as literals.
        "CycleEndDate": "{{ ti.xcom_pull(task_ids='derive_cycle_end_date') }}",
        "CreationTimestamp": "{{ ti.xcom_pull(task_ids='build_creation_timestamp') }}",
        # ... rest from the model owner's spec ...
    },
)
```

The `display_name` is mandatory — Ops uses it to pair SageMaker
executions with the triggering Airflow run.

### 6. Sensors — pick the right tool

| Signal | Use |
|---|---|
| An S3 partition with a `_SUCCESS` / `*.parquet` marker | [`S3SuccessSensorFactory`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/sensor_factory.py) → see [add-sensor-to-dag](../add-sensor-to-dag/SKILL.md) |
| A Redshift table freshness check | [`RedshiftFreshnessSensor`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/redshift_freshness_sensor.py) → see [add-sensor-to-dag](../add-sensor-to-dag/SKILL.md) |
| A partition **shared** with another scoring DAG (e.g., the CCM v5 / CCM sensor set) | `ExternalTaskSensor` waiting on `ccm_score_common_sensors` → see [add-sensor-to-dag](../add-sensor-to-dag/SKILL.md) and [ADR 0001](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/decisions/0001-shared-sensors-dag.md) |
| A pre-decisioning DAG finishing (monitoring waits on scoring) | `ExternalTaskSensor` against the upstream DAG's last task |

Do **not** roll a new sensor class. If the catalog in
[`docs/sensors.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/sensors.md)
genuinely doesn't cover the case, stop and file a design discussion before
adding to `dags/sensors/`.

### 7. Task graph — minimum shape

```python
derived_cycle_end_date >> check_s3_partition_task >> [ *sensors ] >> trigger_sagemaker
```

Model DAGs often also chain a downstream `SQLExecuteQueryOperator` to
refresh a Redshift materialized view. Monitoring DAGs chain the
`resolve_score_creation_timestamp` task between the upstream
`ExternalTaskSensor` and the trigger.

## MWAA capacity check (do this before the PR)

MWAA `mw1.micro` caps the cluster at **25 active DAGs**. Before adding a
new one, confirm with the owner (or check the Airflow UI) that the slot
is available. If not, coordinate deprecation of an old DAG first.

## Testing the DAG

Every new DAG needs a mirrored test file under
`tests/dags/<subfolder>/test_<dag_id>.py`. At minimum, test that:

- The DAG loads without raising.
- `dag_id` matches the filename.
- The schedule matches the expected cron string.
- All expected tasks are present.
- `on_failure_callback` is wired to `DagFailureNotifier`.

See [`test_ccm_score_common_sensors_dag.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/tests/dags/beast_models/test_ccm_score_common_sensors_dag.py)
for the canonical loader pattern (sets `AIRFLOW_VAR_ENVIRONMENT`, loads
the module via `importlib.util.spec_from_file_location`).

Run with:

```bash
pytest tests/dags/<subfolder>/test_<dag_id>.py -v
```

More detail in [`docs/testing.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/testing.md).

## Next steps

Once the DAG file + test pass locally, follow the canonical runbook to
ship it:
[`docs/runbooks/onboarding-a-new-dag.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/runbooks/onboarding-a-new-dag.md)
— covers ownership registration in `dag_owners_config.py`, the PR
template, and deploy verification.

## Resources

### scripts/

- [`scaffold_model_dag.py`](scripts/scaffold_model_dag.py) — generate a
  model DAG skeleton mirroring `ccm_score_v5.py`.
- [`scaffold_feature_dag.py`](scripts/scaffold_feature_dag.py) — generate
  a feature DAG skeleton mirroring `ccm_referral_dag.py`.
- [`scaffold_monitoring_dag.py`](scripts/scaffold_monitoring_dag.py) —
  generate a monitoring DAG skeleton mirroring `cubo_monitor_v5.py`.

### references/

- [`flavors.md`](references/flavors.md) — deeper side-by-side of the
  three DAG flavors and when to use each.
