---
name: beast-pipeline-create-custom-sensor
description: Build a new Airflow sensor class from scratch, or modify the behavior of an existing sensor in dags/sensors/. Use when the existing catalog (S3SuccessCustomSensor, RedshiftFreshnessSensor, OffHoursSensor, etc.) genuinely doesn't cover the signal you need to wait on, when adding a new field or extension to an existing sensor, or when updating the sensor factory. For simply wiring an existing sensor into a DAG, use beast-pipeline-add-sensor-to-dag instead.
---

# Create or Edit a Custom Sensor

This sub-skill is for **MLOps platform work** — extending the sensor
catalog under
[`dags/sensors/`](https://github.com/credifranco/mlops-mwaa-dags/tree/main/dags/sensors).
Audience: MLOps engineers maintaining the BEAST platform. Feature and
model owners should use
[add-sensor-to-dag](../add-sensor-to-dag/SKILL.md) instead — they almost
never need to write a new sensor class.

## Before you write a line of code — justify the new class

Rolling a new sensor class is expensive: it's a platform API surface
that MWAA operators will rely on for years. Stop and answer:

1. **Can this be solved with an existing class + config?** e.g., adding
   a new `fallback_prefix` use case doesn't need a new class, just a
   new entry in the DAG's sensor config list.
2. **Is the signal genuinely novel?** Novel = not "file in S3", "row in
   Redshift", "external DAG finished", or "clock is in a time window"
   — those four patterns are already covered.
3. **Who else needs this?** If only one DAG will ever use it, a
   `@task.sensor`-decorated function inline in that DAG is cheaper than
   a new class. Promote to `dags/sensors/` only when a second caller
   materializes, or when the logic exceeds ~30 lines.

If you can't answer all three, don't add a class — either use an
existing sensor or extend it via a new field (see "Edit existing
sensor" below).

## Decision tree — which base class to extend

| You're waiting on… | Extend | Example in repo |
|---|---|---|
| An S3 object / prefix with some completion marker | `S3KeySensor` (from `airflow.providers.amazon.aws.sensors.s3`) | [`S3SuccessCustomSensor`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/s3_custom_sensors.py) |
| The result of an arbitrary SQL query (Redshift / Postgres / any SQL conn) | `SqlSensor` (from `airflow.providers.common.sql.sensors.sql`) | [`RedshiftFreshnessSensor`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/redshift_freshness_sensor.py) |
| Anything else (wall-clock, API endpoint, Google Sheet, filesystem signal) | `BaseSensorOperator` (from `airflow.sensors.base`) directly | [`OffHoursSensor`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/time_window_sensor.py) |

**Do not** extend the most permissive class (`BaseSensorOperator`) if a
tighter provider-specific base (like `S3KeySensor` or `SqlSensor`)
works — you give up battle-tested behavior for no reason.

## Quick start — scaffold

```bash
# extend an S3 provider sensor (most common)
python scripts/scaffold_custom_sensor.py \
    --class-name S3MultiSuffixSensor \
    --module-file s3_multi_suffix_sensor.py \
    --base-class s3 \
    --output dags/sensors/s3_multi_suffix_sensor.py

# extend SqlSensor (Redshift / Postgres)
python scripts/scaffold_custom_sensor.py \
    --class-name RedshiftRowCountSensor \
    --module-file redshift_row_count_sensor.py \
    --base-class sql \
    --output dags/sensors/redshift_row_count_sensor.py

# extend BaseSensorOperator (from scratch, rare)
python scripts/scaffold_custom_sensor.py \
    --class-name HttpHealthSensor \
    --module-file http_health_sensor.py \
    --base-class base \
    --output dags/sensors/http_health_sensor.py
```

The scaffold emits a minimal subclass with the required elements
(`template_fields`, `__init__` signature, `poke()` stub, docstring
skeleton) wired correctly for the chosen base. Edit the TODO markers.

## Canonical structure of a custom sensor

Regardless of base class, every BEAST sensor must have:

### 1. Module-level docstring + top-of-file shebang imports

```python
"""Custom <one-line purpose> sensor utility."""

from __future__ import annotations

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.decorators import apply_defaults
```

### 2. `template_fields` — any field rendered with Jinja at runtime

If a DAG passes `f"...dt={derived_cycle_end_date}"` as `prefix`, that
field must be in `template_fields` or the XCom rendering won't happen.

```python
class S3SuccessCustomSensor(S3KeySensor):
    template_fields = ("bucket_key", "bucket_name", "prefix", "fallback_prefix")
```

Inherit from the base's `template_fields` when adding — don't replace:

```python
class RedshiftFreshnessSensor(SqlSensor):
    template_fields = (*SqlSensor.template_fields, "table", "column", "condition")
```

### 3. `__init__` with keyword-only args and `**kwargs` passthrough

Always use keyword-only args (`*` in the signature) for everything the
caller sets — it prevents positional-arg bugs when a field is added
later. Pass `**kwargs` through to `super().__init__`.

```python
def __init__(
    self,
    *,
    bucket_name: str,
    prefix: str,
    success_suffix: str = "_SUCCESS",
    fallback_prefix: str | None = None,
    aws_conn_id: str = "aws_default",
    **kwargs,
):
    # normalize inputs
    self.prefix = prefix.rstrip("/") + "/" if prefix else ""
    self.success_suffix = success_suffix
    self.fallback_prefix = fallback_prefix.rstrip("/") + "/" if fallback_prefix else None
    # build base-class args
    bucket_key = f"{self.prefix}*{success_suffix}"
    super().__init__(
        bucket_key=bucket_key,
        bucket_name=bucket_name,
        wildcard_match=True,
        aws_conn_id=aws_conn_id,
        **kwargs,
    )
```

Note `@apply_defaults` is **not** needed in Airflow 2.x — it's a no-op
decorator now. The existing `S3SuccessCustomSensor` still has it for
back-compat; don't add it to new sensors.

### 4. `poke()` — the method that returns True when the condition is met

The contract: **return `True`** when the DAG should proceed, **`False`**
otherwise. Never raise except for unrecoverable errors.

```python
def poke(self, context) -> bool:
    self.log.info(f"Checking for *{self.success_suffix} in s3://{self.bucket_name}/{self.prefix}")
    result = super().poke(context)
    if result:
        self.log.info("Marker found — sensing complete.")
        return True
    # ... fallback logic, if any ...
    return False
```

Use `self.log.info` for progress. Do **not** use `print`.

### 5. MWAA compatibility — never hold an executor slot

The caller is responsible for passing `mode="reschedule"`, but your
sensor should be **compatible** with it: `poke()` must be stateless
between calls. Never store partial progress on `self` that the next
poke relies on. If you need continuation state, look at
[`SqlSensor`'s `mode` handling](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/_modules/airflow/providers/common/sql/sensors/sql.html)
for patterns.

On `mw1.micro` the DAG-cap is 25 and the cluster-wide parallelism is 32
— a misbehaving sensor that holds a slot is an outage.

## Edit existing sensor behavior

Modifying an existing sensor is usually **better** than creating a new
one. Common safe changes:

### Adding a new optional field

Example: adding `fallback_prefix` to `S3SuccessCustomSensor`.

1. Add the new kwarg with a default value so all existing callers keep
   working:
   ```python
   def __init__(
       self,
       *,
       fallback_prefix: str | None = None,
       ...,
       **kwargs,
   ):
       self.fallback_prefix = fallback_prefix.rstrip("/") + "/" if fallback_prefix else None
       ...
   ```
2. If the field is Jinja-rendered, **add it to `template_fields`**.
3. Add the same field to the `S3SuccessSensorFactory` allow-list in
   [`sensor_factory.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/sensor_factory.py) —
   `_create_sensor()` uses explicit kwargs, so any new field needs a
   line there too.
4. Update the docstring examples.
5. Add a unit test covering the new field.
6. Update
   [`docs/sensors.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/sensors.md)
   — the catalog is the entry point everyone reads.

### Changing SQL inside `RedshiftFreshnessSensor`

Non-trivial. The sensor builds the query in `__init__` — a change there
affects every DAG using it. Before editing:

1. Search for callers: `rg "RedshiftFreshnessSensor" dags/`.
2. Confirm every caller is compatible with the new SQL shape.
3. Consider adding a new optional kwarg (`min_rows`, `order_by`, etc.)
   instead of mutating the default.
4. Dry-run the new SQL against Redshift directly before shipping.

## Register the sensor for easy import

Sensors are currently imported per-file:

```python
from sensors.sensor_factory import S3SuccessSensorFactory
from sensors.redshift_freshness_sensor import RedshiftFreshnessSensor
```

No `__init__.py` re-exports today. If you add a new sensor, match the
same pattern — do **not** re-export from `sensors/__init__.py` unless
you're also updating every existing DAG (touching too many files for a
single PR).

## Factory integration

If the new sensor follows the "config-driven list of sensors" pattern
(more than one upstream partition in a single DAG), extend
[`S3SuccessSensorFactory`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/sensor_factory.py)
rather than building a new factory class.

The factory pattern is only justified for sensors that:
- are instantiated in bulk (DAGs wire 3+ of them in a `sensor_configs`
  list), and
- share more than two common default values across calls.

A sensor called once per DAG doesn't need a factory.

## Testing

Custom sensors need **unit tests**, not just DAG-load tests. The test
file goes under `tests/dags/sensors/test_<sensor_module>.py`. Mirror
the structure of the existing
[`test_ccm_score_common_sensors_dag.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/tests/dags/beast_models/test_ccm_score_common_sensors_dag.py)
loader pattern — set `AIRFLOW_VAR_ENVIRONMENT=prod`, use
`importlib.util.spec_from_file_location` — then:

1. **Initializer test** — the sensor instantiates with minimal args.
2. **template_fields test** — declared Jinja-renderable fields are in
   `template_fields`.
3. **poke() positive test** — mock the hook / SQL response that should
   return `True`.
4. **poke() negative test** — mock the same with a response that should
   return `False`.
5. **Edge case** — timeout / fallback / missing conn.

Mock at the boundary: patch `S3Hook.check_for_prefix`, not the whole
`S3KeySensor.poke`. This keeps the test grounded in the real class
hierarchy.

## Update docs/sensors.md

The catalog in
[`docs/sensors.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/sensors.md)
is the single entry point for feature/model owners choosing a sensor.
Every new sensor class **must** get an entry with:

- Class name and module path.
- One-line purpose.
- Minimal example (parameters).
- When to use / when not to use.
- Link back to the sensor's docstring.

A new sensor that's not in `docs/sensors.md` is invisible to the team.

## Deploy / rollout checklist

Same as any DAG change — there's no special deploy step for a new
sensor file. Once merged, MWAA syncs `dags/` from S3 on its next
parse cycle. But because a buggy sensor can brick every DAG that
imports it, prefer this rollout:

1. Land the sensor class + its unit test in a dedicated PR (no DAG
   changes).
2. Verify MWAA parse passes on the dev environment.
3. Land the DAG-level adoption in a follow-up PR, one or two DAGs at a
   time.

## Anchors in the repo

- [`s3_custom_sensors.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/s3_custom_sensors.py) — canonical S3 extension.
- [`redshift_freshness_sensor.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/redshift_freshness_sensor.py) — canonical SQL extension.
- [`time_window_sensor.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/time_window_sensor.py) — canonical `BaseSensorOperator` extension.
- [`sensor_factory.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/sensors/sensor_factory.py) — the factory to extend (don't duplicate).

## Resources

### scripts/

- [`scaffold_custom_sensor.py`](scripts/scaffold_custom_sensor.py) —
  generate a sensor skeleton for any of the three base classes.

### references/

- [`base-class-comparison.md`](references/base-class-comparison.md) —
  side-by-side of S3KeySensor vs SqlSensor vs BaseSensorOperator and
  when to pick each.
