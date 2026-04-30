#!/usr/bin/env python3
"""
Scaffold a SageMaker pipeline with NotebookJobStep from a notebook path.

Usage:
    python scaffold_pipeline.py --notebook path/to/notebook.ipynb --pipeline-name my-pipeline
    python scaffold_pipeline.py --notebook s3://bucket/notebook.ipynb --pipeline-name my-pipeline --params company=Amazon

Outputs Python code to stdout. Copy into your project and fill in image_uri, role, etc.
"""

import argparse


def scaffold(
    notebook_path: str,
    pipeline_name: str,
    step_name: str | None = None,
    params: dict | None = None,
) -> str:
    step = step_name or "notebook_step"
    params_str = ""
    if params:
        params_repr = repr(params)
        params_str = f",\n    parameters={params_repr}"

    return f'''#!/usr/bin/env python3
"""SageMaker pipeline with NotebookJobStep - scaffolded from {notebook_path}"""

from sagemaker.workflow.steps import NotebookJobStep
from sagemaker.workflow.pipelines import Pipeline
import sagemaker

# Configuration - fill in these values
NOTEBOOK_URI = "{notebook_path}"
IMAGE_URI = "YOUR_ECR_IMAGE_URI"  # See AWS image constraints for notebook jobs
KERNEL_NAME = "python3"
ROLE_ARN = "arn:aws:iam::ACCOUNT:role/YourSageMakerRole"
SAGEMAKER_SESSION = sagemaker.Session()

notebook_step = NotebookJobStep(
    name="{step}",
    input_notebook=NOTEBOOK_URI,
    image_uri=IMAGE_URI,
    kernel_name=KERNEL_NAME,
    role=ROLE_ARN{params_str}
)

pipeline = Pipeline(
    name="{pipeline_name}",
    steps=[notebook_step],
    sagemaker_session=SAGEMAKER_SESSION,
)

if __name__ == "__main__":
    pipeline.upsert(role_arn=ROLE_ARN)
'''


def parse_params(params: list[str]) -> dict:
    result = {}
    for p in params:
        if "=" in p:
            k, v = p.split("=", 1)
            result[k.strip()] = v.strip()
    return result


def main():
    parser = argparse.ArgumentParser(description="Scaffold SageMaker pipeline from notebook")
    parser.add_argument("--notebook", required=True, help="Path or S3 URI to notebook")
    parser.add_argument("--pipeline-name", required=True, help="Pipeline name")
    parser.add_argument("--step-name", help="Step name (default: notebook_step)")
    parser.add_argument("--params", nargs="*", metavar="KEY=VALUE", help="Notebook parameters")
    args = parser.parse_args()

    params = parse_params(args.params) if args.params else None
    code = scaffold(
        notebook_path=args.notebook,
        pipeline_name=args.pipeline_name,
        step_name=args.step_name,
        params=params,
    )
    print(code)


if __name__ == "__main__":
    main()
