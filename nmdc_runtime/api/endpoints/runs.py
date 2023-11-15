from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import concat

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import _request_dagster_run
from nmdc_runtime.api.models.run import (
    RunSummary,
    RunEvent,
    RunUserSpec,
)
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.util import ListResponse

router = APIRouter()


@router.post("/runs", response_model=RunSummary)
def request_run(
    run_user_spec: RunUserSpec = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    requested = _request_dagster_run(
        nmdc_workflow_id=run_user_spec.job_id,
        nmdc_workflow_inputs=run_user_spec.inputs,
        extra_run_config_data=run_user_spec.run_config,
        mdb=mdb,
        user=user,
    )
    if requested["type"] == "success":
        return _get_run_summary(requested["detail"]["run_id"], mdb)
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                f"Runtime failed to start {run_user_spec.job_id} job. "
                f'Detail: {requested["detail"]}'
            ),
        )


def _get_run_summary(run_id, mdb) -> RunSummary:
    events_in_order = list(mdb.run_events.find({"run.id": run_id}, sort=[("time", 1)]))
    raise404_if_none(events_in_order or None)
    # TODO put relevant outputs in outputs! (for get_study_metadata job)
    return RunSummary(
        id=run_id,
        status=events_in_order[-1]["type"],
        started_at_time=events_in_order[0]["time"],
        was_started_by=events_in_order[0]["producer"],
        inputs=list(concat(e["inputs"] for e in events_in_order)),
        outputs=list(concat(e["outputs"] for e in events_in_order)),
        job=events_in_order[-1]["job"],
        producer=events_in_order[-1]["producer"],
        schemaURL=events_in_order[-1]["schemaURL"],
    )


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
    mdb.run_events.insert_one(run_event.model_dump())
    return _get_run_summary(run_event.run.id, mdb)
