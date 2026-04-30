---
name: beast-index
description: Platform skill for onboarding onto BEAST — Stori's batch Pre-decisioning Platform built on AWS MWAA (Airflow) orchestrating SageMaker Pipelines. Use when the user asks about creating a new DAG for a model, feature, or monitoring workflow; adding S3 or Redshift sensors; wiring cross-DAG dependencies via ExternalTaskSensor; or shipping a DAG through the PR/deploy process. Routes to the correct sub-skill based on the user's goal.
---

# BEAST Pipeline (DSML MLOps Platform)

BEAST (Batch Execution for All Scoring Tasks) is Stori's centralized
Pre-decisioning Platform. It runs on AWS MWAA (Airflow 2.10.3, `mw1.micro`)
and orchestrates SageMaker Pipelines for feature engineering, model
scoring, and monitoring. Airflow is a **pure orchestrator** — every
heavy lifting step runs inside SageMaker.

This skill routes BEAST questions to the correct sub-skill.

## Golden rules (apply to every sub-skill)

These are project non-negotiables. They hold regardless of which sub-skill
you load.

- Resource names embed `ENVIRONMENT` from `Variable.get("ENVIRONMENT")` —
  never hard-code `dev` or `prod`.
- `start_date` uses `pendulum.timezone("America/Mexico_City")`.
- `dag_id=os.path.basename(__file__).replace(".py", "")`.
- Every DAG wires `on_failure_callback=[DagFailureNotifier(owner="<slack-owner>").get_callback()]`.
  The owner must exist in
  [`dag_owners_config.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/utils/dag_owners_config.py).
- Guard every SageMaker trigger with a `@task.short_circuit`
  `check_if_partition_exists` that skips the run if the output partition
  is already there.
- Use `S3SuccessSensorFactory` for S3 sensing; use `RedshiftFreshnessSensor`
  for Redshift freshness. Don't invent new sensor classes.
- `SageMakerStartPipelineOperator` always with `wait_for_completion=True`
  and `display_name=f"{PIPELINE}-airflow-triggered-execution-{{{{ ts_nodash }}}}"`.
- Secrets come from AWS Secrets Manager or Airflow connections — never
  from `Variables` or `pipeline_params`.
- Comments explain **why**, never what. No inline narration of
  self-evident code.

Reference: [`mlops-mwaa-dags/docs/conventions.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/conventions.md).

## Routing Table

Read the user's goal and load the matching sub-skill. Do not answer from
this file alone.

| User goal | Sub-skill to load |
|-----------|-------------------|
| Onboard a new model scoring DAG (lives in `dags/beast_models/`) | [create-dag](create-dag/SKILL.md) — use the **model** flavor |
| Onboard a new feature engineering DAG (lives in `dags/beast_features/`) | [create-dag](create-dag/SKILL.md) — use the **feature** flavor |
| Onboard a new monitoring DAG (lives in `dags/beast_model_monitoring/`) | [create-dag](create-dag/SKILL.md) — use the **monitoring** flavor |
| Add an **existing** S3 `_SUCCESS` sensor to a DAG | [add-sensor-to-dag](add-sensor-to-dag/SKILL.md) — S3 section |
| Add an **existing** Redshift freshness sensor to a DAG | [add-sensor-to-dag](add-sensor-to-dag/SKILL.md) — Redshift section |
| Wire a cross-DAG dependency (`ExternalTaskSensor`) | [add-sensor-to-dag](add-sensor-to-dag/SKILL.md) — Cross-DAG section |
| **Create** a new sensor class from scratch (platform work) | [create-custom-sensor](create-custom-sensor/SKILL.md) |
| **Edit** behavior of an existing sensor class in `dags/sensors/` | [create-custom-sensor](create-custom-sensor/SKILL.md) — Edit existing section |
| Understand where a DAG file should live, register ownership, open the PR | See [`docs/runbooks/onboarding-a-new-dag.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/runbooks/onboarding-a-new-dag.md) |

## How to use this skill

1. Identify the user's primary BEAST goal from the table above.
2. Load and follow the matched sub-skill. Sub-skills contain the actual
   scaffolds, commands, and step-by-step instructions.
3. If the goal spans sub-skills, load them in the right layering:
   - **Author a DAG:** `create-dag` → `add-sensor-to-dag`.
   - **Extend the platform:** `create-custom-sensor` produces a new
     sensor primitive; `create-dag` and `add-sensor-to-dag` then
     consume it.
   For the PR / deploy process, follow
   [`docs/runbooks/onboarding-a-new-dag.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/runbooks/onboarding-a-new-dag.md).
4. If no sub-skill matches, ask the user to clarify their intent before
   writing any code.

## Upstream references

When you need more context than a sub-skill provides, read the source
docs directly:

- [`docs/architecture.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/architecture.md) — platform end-to-end.
- [`docs/dag-patterns.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/dag-patterns.md) — canonical DAG shapes.
- [`docs/sensors.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/sensors.md) — sensor catalog.
- [`docs/testing.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/testing.md) — test conventions.
- [`docs/runbooks/onboarding-a-new-dag.md`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/runbooks/onboarding-a-new-dag.md) — the canonical runbook.
- [`docs/decisions/`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/docs/decisions) — ADRs for non-obvious platform choices.

The anchor DAG for every new model is
[`ccm_score_v5.py`](https://github.com/credifranco/mlops-mwaa-dags/blob/main/dags/beast_models/ccm_score_v5.py) —
mirror it whenever the conventions leave a gap.
