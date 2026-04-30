---
name: sagemaker-notebookjob-to-pipeline
description: Create and manage SageMaker Pipelines that run Jupyter notebooks as NotebookJobStep. Use when converting a notebook into a pipeline, scheduling notebook execution, building multi-step ML workflows from notebooks, passing parameters to notebooks, or setting up on-demand or cron-scheduled runs. Covers NotebookJobStep, Pipeline, PipelineSchedule, Papermill parameters, and image constraints.
---

# SageMaker NotebookJob to Pipeline

## Overview

Convert Jupyter notebooks into SageMaker Pipelines using `NotebookJobStep`. Run notebooks on demand, schedule them (cron or one-time), and chain multiple notebooks into DAG workflows—without refactoring notebook code into Python modules.

## Quick Start

1. Create a `NotebookJobStep` with `input_notebook`, `image_uri`, and `kernel_name`.
2. Create a `Pipeline` with the step.
3. Call `pipeline.upsert()` then `pipeline.start()` or `pipeline.put_triggers()`.

```python
from sagemaker.workflow.steps import NotebookJobStep
from sagemaker.workflow.pipelines import Pipeline
import sagemaker

step = NotebookJobStep(
    name="my-step",
    input_notebook="s3://bucket/my-notebook.ipynb",
    image_uri="<ecr-image-uri>",
    kernel_name="python3",
    role="arn:aws:iam::ACCOUNT:role/MyRole",
)

pipeline = Pipeline(
    name="my-notebook-pipeline",
    steps=[step],
    sagemaker_session=sagemaker.Session(),
)

pipeline.upsert(role_arn=step.role)
execution = pipeline.start()
```

## Workflow Decision Tree (Routing to the Right Section)

| Goal | Section |
|------|---------|
| Single notebook → pipeline | [Single-Step Pipeline](#single-step-pipeline) |
| Pass parameters to notebook (optional) | [Parameters](#parameters) → [references/notebook-requirements.md](references/notebook-requirements.md) |
| Schedule (cron or one-time, self-contained) | [Scheduling](#scheduling) → [references/notebook-job-step-api.md](references/notebook-job-step-api.md) |
| Schedule with sensors or pre-steps | [Scheduling via Beast Pipeline](#scheduling-via-beast-pipeline) |
| Multiple notebooks in DAG | [Multi-Step Pipeline](#multi-step-pipeline) |
| Secrets, image constraints | [references/notebook-job-step-api.md](references/notebook-job-step-api.md), [references/notebook-requirements.md](references/notebook-requirements.md) |

## Single-Step Pipeline

Use the scaffold script for a starting template:

```bash
python scripts/scaffold_pipeline.py --notebook s3://bucket/train.ipynb --pipeline-name my-pipeline
```

Fill in `IMAGE_URI` and `ROLE_ARN`. For image constraints, see [references/notebook-requirements.md](references/notebook-requirements.md).

## Parameters

**Parameters are optional.** Only add them if the notebook needs runtime values injected at execution time.

To pass parameters via `NotebookJobStep(parameters={...})`, the notebook must have a cell tagged `parameters` (Papermill convention). See [references/notebook-requirements.md](references/notebook-requirements.md).

Scaffold with params:

```bash
python scripts/scaffold_pipeline.py --notebook s3://bucket/train.ipynb --pipeline-name my-pipeline --params company=Amazon model_id=v2
```

## Scheduling

Use SageMaker-native scheduling when the pipeline is **self-contained** — no upstream sensors, data availability checks, or custom pre-processing steps required.

- **On-demand**: `pipeline.start()`
- **One-time future**: `PipelineSchedule(at=datetime(...))`
- **Recurring**: `PipelineSchedule(cron="15 10 * * ? *")`

See [references/notebook-job-step-api.md](references/notebook-job-step-api.md) for `PipelineSchedule` usage.

## Scheduling via Beast Pipeline

When the SageMaker pipeline needs to run **after** sensors, data checks, or custom script steps, use the internal **Beast Pipeline** (Airflow-based orchestrator managed by the MLOps team) instead of — or in combination with — SageMaker-native scheduling.

Beast Pipeline is the right choice when:

- The pipeline depends on an upstream data sensor (e.g., wait for S3 file, table partition, or API signal).
- A custom pre-processing or validation script must run before the SageMaker pipeline starts.
- The pipeline is part of a broader cross-system workflow requiring Airflow-style DAG orchestration.

In this pattern, Beast Pipeline triggers `SageMakerStartPipelineOperator` (or an equivalent API call) as one of its DAG tasks, keeping SageMaker responsible for notebook execution while Airflow handles orchestration logic.

> Contact the MLOps team to integrate your SageMaker pipeline into Beast Pipeline (Airflow cluster).

## Multi-Step Pipeline

Add multiple `NotebookJobStep` instances to `Pipeline(steps=[...])`. Dependencies and ordering follow the step list; use step outputs or conditions as needed for DAG structure.

## Resources

### scripts/

- **scaffold_pipeline.py** – Generates pipeline Python code from notebook path. Use for consistent boilerplate.

### references/

- **[notebook-job-step-api.md](references/notebook-job-step-api.md)** – NotebookJobStep, Pipeline, PipelineSchedule API patterns, parameters, scheduling.
- **[notebook-requirements.md](references/notebook-requirements.md)** – Papermill parameters cell, image constraints, Studio UI tags.
