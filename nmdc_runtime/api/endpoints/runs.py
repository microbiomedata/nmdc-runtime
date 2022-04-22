from dagster import PipelineRunStatus
from dagster_graphql import DagsterGraphQLClientError
from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import concat, merge

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import raise404_if_none, pick, now
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.run import (
    RunRequest,
    RunSummary,
    RunEvent,
    Run,
    get_dagster_graphql_client,
)
from nmdc_runtime.api.models.util import ListRequest, ListResponse

router = APIRouter()

PRODUCER_URL_BASE_DEFAULT = (
    "https://github.com/microbiomedata/nmdc-runtime/tree/main/nmdc_runtime/"
)
SCHEMA_URL_BASE_DEFAULT = (
    "https://github.com/microbiomedata/nmdc-runtime/tree/main/nmdc_runtime/"
)

PRODUCER_URL = PRODUCER_URL_BASE_DEFAULT.replace("/main/", "/v0-0-1/") + "producer"
SCHEMA_URL = SCHEMA_URL_BASE_DEFAULT.replace("/main/", "/v0-0-1/") + "schema.json"


@router.post("/runs", response_model=RunSummary)
def request_run(
    run_request: RunRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    # XXX what we consider a "job" here, is currently a "workflow" elsewhere...
    job = raise404_if_none(mdb.workflows.find_one({"id": run_request.job_id}))
    run_id = generate_one_id(mdb, "runs")
    event = RunEvent(
        producer="user",
        schemaURL=SCHEMA_URL,
        run=Run(id=run_id),
        job=merge(
            pick(["id", "description"], job),
            {"producer": PRODUCER_URL, "schemaURL": SCHEMA_URL},
        ),
        type="REQUESTED",
        time=now(as_str=True),
        inputs=run_request.inputs,
    )
    mdb.run_events.insert_one(event.dict())
    requested = mdb.run_events.find_one({"run.id": run_id}, sort=[("time", -1)])
    mdb.run_events.insert_one(
        RunEvent(
            producer=PRODUCER_URL,
            schemaURL=SCHEMA_URL,
            run=requested["run"],
            job=requested["job"],
            type="STARTED",
            time=now(as_str=True),
            inputs=[],
        ).dict()
    )
    started = mdb.run_events.find_one({"run.id": run_id}, sort=[("time", -1)])
    # TODO request async work and put details here. worker will complete.
    mdb.run_events.insert_one(
        RunEvent(
            producer=PRODUCER_URL,
            schemaURL=SCHEMA_URL,
            run=started["run"],
            job=started["job"],
            type="COMPLETED",
            time=now(as_str=True),
            inputs=[],
            outputs=["PARTY"],
        ).dict()
    )
    return _get_run_summary(run_id, mdb)


def _get_run_summary(run_id, mdb):
    events_in_order = list(mdb.run_events.find({"run.id": run_id}, sort=[("time", 1)]))
    return {
        "id": run_id,
        "status": events_in_order[-1]["type"],
        "started_at_time": events_in_order[0]["time"],
        "was_started_by": events_in_order[0]["producer"],
        "inputs": list(concat(e["inputs"] for e in events_in_order)),
        "outputs": list(concat(e["outputs"] for e in events_in_order)),
        "job": events_in_order[-1]["job"],
        "producer": events_in_order[-1]["producer"],
        "schemaURL": events_in_order[-1]["schemaURL"],
    }


@router.get(
    "/runs/{run_id}", response_model=RunSummary, response_model_exclude_unset=True
)
def get_run_summary(
    run_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return _get_run_summary(run_id, mdb)


@router.get("/runs/{run_id}/events", response_model=ListResponse[RunEvent])
def list_events_for_run(
    run_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """List events for run, in reverse chronological order."""
    raise404_if_none(mdb.run_events.find_one({"run.id": run_id}))
    return {
        "resources": list(mdb.run_events.find({"run.id": run_id}, sort=[("time", -1)]))
    }


@router.post(
    "/runs/{run_id}/events", response_model=RunEvent, response_model_exclude_unset=True
)
def post_run_event(
    run_id: str,
    run_event: RunEvent = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    if run_id != run_event.run.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Supplied run_event.run.id does not match run_id given in request URL.",
        )
    mdb.run_events.insert_one(run_event.dict())
    return _get_run_summary(run_event.run.id, mdb)


@router.get("/run-events", response_model=ListResponse[RunEvent])
def list_run_events(
    req: ListRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return list_resources(req, mdb, "run_events")


def _request_dagster_run(
    job_name: str,
    run_config_data: dict,
    repository_location_name="nmdc_runtime.site.repository:repo",
    repository_name="repo",
):
    """
    Example 1:
    - job_name: hello_job
    - run_config_data: {"ops": {"hello": {"config": {"name": "Donny"}}}}

    Example 2:
    - job_name: hello_job
    - run_config_data: {}
    """
    dagster_client = get_dagster_graphql_client()
    try:
        run_id: str = dagster_client.submit_job_execution(
            job_name,
            repository_location_name=repository_location_name,
            repository_name=repository_name,
            run_config=run_config_data,
        )
        return {"type": "success", "detail": {"run_id": run_id}}
    except DagsterGraphQLClientError as exc:
        return {"type": "error", "detail": str(exc)}


def _get_dagster_run_status(run_id: str):
    dagster_client = get_dagster_graphql_client()
    try:
        run_status: PipelineRunStatus = dagster_client.get_run_status(run_id)
        return {"type": "success", "detail": str(run_status.value)}
    except DagsterGraphQLClientError as exc:
        return {"type": "error", "detail": str(exc)}
