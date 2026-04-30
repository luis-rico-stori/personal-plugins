# Notebook Requirements for NotebookJobStep

## Table of Contents
- [Parameters Cell (Papermill)](#parameters-cell-papermill)
- [Image Constraints](#image-constraints)
- [EMR Connections](#emr-connections)
- [Studio UI Visibility](#studio-ui-visibility)

---

## Parameters Cell (Papermill)

To pass parameters to the notebook via `NotebookJobStep(parameters={...})`, the notebook **must** have a cell tagged `parameters`.

1. Add a cell near the top of the notebook.
2. Define default variables in that cell.
3. Tag the cell with `parameters` in Jupyter (Edit → Cell → Cell Tags, or View → Cell Toolbar → Tags).

Example parameters cell:

```python
# Default parameters - Papermill will override these
company = "default"
model_id = "v1"
threshold = 0.5
```

When the pipeline runs, Papermill injects the values from `NotebookJobStep(parameters={"company": "Amazon", ...})` immediately after this cell.

---

## Image Constraints

When scheduling notebook jobs with the SageMaker Python SDK, only certain images are supported. Check [AWS Image constraints for notebook jobs](https://github.com/aws/sagemaker-distribution/blob/main/support_policy.md#supported-image-versions).

- Use SageMaker-managed images or approved ECR images.
- Match the image to the kernel (e.g., `python3` kernel requires Python 3 image).

---

## Studio UI Visibility

To see notebook jobs in the SageMaker Studio Notebook Jobs dashboard, add tags to `NotebookJobStep`:

| Tag Key | Tag Value | Effect |
|---------|-----------|--------|
| `sagemaker:domain-name` | `d-xxxxxxxx` | Visible to all in domain |
| `sagemaker:user-profile-name` | `studio-user` | Visible to specific user (requires domain tag) |
| `sagemaker:shared-space-name` | `my-space` | Visible to space (requires domain tag) |

Without these tags, jobs appear in the pipeline executions list but not in the Notebook Jobs dashboard.
