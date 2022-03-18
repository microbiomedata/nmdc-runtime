import os
from http.client import HTTPException

from dagster import PipelineRunStatus
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError
from fastapi import APIRouter
from starlette import status

router = APIRouter()

DAGIT_HOSTNAME, DAGIT_PORT = os.getenv("DAGIT_HOST").split("://", 1)[-1].split(":", 1)
DAGIT_PORT = int(DAGIT_PORT)


@router.post("/runs:request")
def request_run(job_name: str, run_config_data: dict):
    """
    Example 1:
    - job_name: hello_job
    - run_config_data: {"ops": {"hello": {"config": {"name": "Donny"}}}}

    Example 2:
    - job_name: hello_job
    - run_config_data: {}
    """
    client = DagsterGraphQLClient(DAGIT_HOSTNAME, port_number=DAGIT_PORT)
    try:
        new_run_id: str = client.submit_job_execution(
            job_name,
            repository_location_name="nmdc_runtime.site.repository:repo",
            repository_name="repo",
            run_config=run_config_data,
        )
        return {"new_run_id": new_run_id}
    except DagsterGraphQLClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED, detail=str(exc)
        )


@router.get("/runs/{run_id}/status")
def get_run_status(run_id: str):
    client = DagsterGraphQLClient(DAGIT_HOSTNAME, port_number=DAGIT_PORT)

    try:
        run_status: PipelineRunStatus = client.get_run_status(run_id)
        return str(run_status.value)
    except DagsterGraphQLClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED, detail=str(exc)
        )
