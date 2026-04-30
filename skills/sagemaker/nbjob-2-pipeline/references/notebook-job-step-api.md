# NotebookJobStep and Pipeline API Reference

## Table of Contents
- [NotebookJobStep](#notebookjobstep)
- [Pipeline](#pipeline)
- [PipelineSchedule](#pipelineschedule)
- [Running and Scheduling](#running-and-scheduling)

---

## NotebookJobStep

Create a pipeline step that runs a Jupyter notebook as a job. Import from `sagemaker.workflow.steps`.

### Minimum Required Parameters

```python
from sagemaker.workflow.steps import NotebookJobStep

notebook_job_step = NotebookJobStep(
    name="my-notebook-step",
    input_notebook="s3://bucket/path/to/notebook.ipynb",  # or local path in some contexts
    image_uri="<ecr-image-uri>",  # See image constraints
    kernel_name="python3",
)
```

### Common Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | str | Step name (required for pipeline steps) |
| `input_notebook` | str | S3 URI or path to the notebook |
| `image_uri` | str | ECR image URI (see AWS image constraints) |
| `kernel_name` | str | Jupyter kernel (e.g., `python3`, `conda_python3`) |
| `role` | str | IAM role ARN for execution |
| `parameters` | dict | Parameters passed to notebook via Papermill |
| `s3_root_uri` | str | S3 root for outputs |
| `instance_type` | str | Instance type (e.g., `ml.m5.xlarge`) |
| `instance_count` | int | Number of instances |
| `volume_size` | int | EBS volume size in GB |
| `tags` | list | Tags for Studio UI visibility |

### Passing Parameters to Notebook

Notebook must have a cell tagged `parameters`. Papermill injects values after that cell.

```python
notebook_job_step = NotebookJobStep(
    name="parameterized-step",
    input_notebook="s3://bucket/train.ipynb",
    image_uri=image_uri,
    kernel_name="python3",
    parameters={"company": "Amazon", "model_id": "v2"},
)
```

### Default Options (Config File)

Set defaults in `~/.sagemaker/config.yaml` to avoid repeating:

```yaml
SageMaker:
  PythonSDK:
    Modules:
      NotebookJob:
        RoleArn: 'arn:aws:iam::555555555555:role/MyRole'
        S3RootUri: 's3://my-bucket/my-project'
        VpcConfig:
          SecurityGroupIds: ['sg-xxx']
          Subnets: ['subnet-xxx']
```

---

## Pipeline

Create a pipeline and add one or more steps. Import from `sagemaker.workflow.pipelines`.

### Single-Step Pipeline

```python
from sagemaker.workflow.pipelines import Pipeline

pipeline = Pipeline(
    name="my-notebook-pipeline",
    steps=[notebook_job_step],
    sagemaker_session=sagemaker_session,
)
```

### Multi-Step DAG

Chain steps with dependencies. Use `get_step` or pass steps that reference outputs of prior steps.

```python
step1 = NotebookJobStep(name="preprocess", ...)
step2 = NotebookJobStep(name="train", ...)  # Can depend on step1 outputs

pipeline = Pipeline(
    name="multi-step-pipeline",
    steps=[step1, step2],
    sagemaker_session=sagemaker_session,
)
```

---

## PipelineSchedule

Schedule pipeline runs. Import from `sagemaker.workflow.triggers` (or equivalent in your SDK version).

### One-Time Future Run

```python
from datetime import datetime
from sagemaker.workflow.triggers import PipelineSchedule

schedule = PipelineSchedule(
    name="one-time-run",
    at=datetime(year=2024, month=12, day=25, hour=10, minute=31, second=32),
)
pipeline.put_triggers(triggers=[schedule])
```

### Recurring (Cron)

```python
# Every day at 10:15 UTC
schedule = PipelineSchedule(
    name="daily-run",
    cron="15 10 * * ? *",
)
pipeline.put_triggers(triggers=[schedule])
```

Cron format: `minute hour day-of-month month day-of-week year`. See [AWS Scheduler Cron](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html#cron-based).

---

## Running and Scheduling

### On-Demand Run

```python
execution = pipeline.start(
    parameters={}  # Optional pipeline parameters
)
execution.wait()
```

### Create Pipeline Definition (Upsert)

```python
pipeline.upsert(role_arn=role_arn)
```

Run `upsert` before `start` or `put_triggers` to register the pipeline in SageMaker.
