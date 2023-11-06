from enum import Enum
import os
from functools import lru_cache
from typing import List, Optional

from dagster_graphql import DagsterGraphQLClient
from pydantic import BaseModel
from pymongo.database import Database as MongoDatabase
from toolz import merge

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, raise404_if_none, pick
from nmdc_runtime.api.models.user import User

PRODUCER_URL_BASE_DEFAULT = (
    "https://github.com/microbiomedata/nmdc-runtime/tree/main/nmdc_runtime/"
)
SCHEMA_URL_BASE_DEFAULT = (
    "https://github.com/microbiomedata/nmdc-runtime/tree/main/nmdc_runtime/"
)

PRODUCER_URL = PRODUCER_URL_BASE_DEFAULT.replace("/main/", "/v0-0-1/") + "producer"
SCHEMA_URL = SCHEMA_URL_BASE_DEFAULT.replace("/main/", "/v0-0-1/") + "schema.json"


class OpenLineageBase(BaseModel):
    producer: str
    schemaURL: str


class RunUserSpec(BaseModel):
    job_id: str
    run_config: dict = {}
    inputs: List[str] = []


class JobSummary(OpenLineageBase):
    id: str
    description: str


class Run(BaseModel):
    id: str
    facets: Optional[dict] = None


class RunEventType(str, Enum):
    REQUESTED = "REQUESTED"
    STARTED = "STARTED"
    FAIL = "FAIL"
    COMPLETE = "COMPLETE"


class RunSummary(OpenLineageBase):
    id: str
    status: RunEventType
    started_at_time: str
    was_started_by: str
    inputs: List[str]
    outputs: List[str]
    job: JobSummary


class RunEvent(OpenLineageBase):
    run: Run
    job: JobSummary
    type: RunEventType
    time: str
    inputs: Optional[List[str]] = []
    outputs: Optional[List[str]] = []


@lru_cache
def get_dagster_graphql_client() -> DagsterGraphQLClient:
    hostname, port_str = os.getenv("DAGIT_HOST").split("://", 1)[-1].split(":", 1)
    port_number = int(port_str)
    return DagsterGraphQLClient(hostname=hostname, port_number=port_number)


def _add_run_requested_event(run_spec: RunUserSpec, mdb: MongoDatabase, user: User):
    # XXX what we consider a "job" here, is currently a "workflow" elsewhere...
    job = raise404_if_none(mdb.workflows.find_one({"id": run_spec.job_id}))
    run_id = generate_one_id(mdb, "runs")
    event = RunEvent(
        producer=user.username,
        schemaURL=SCHEMA_URL,
        run=Run(id=run_id, facets={"nmdcRuntime_runConfig": run_spec.run_config}),
        job=merge(
            pick(["id", "description"], job),
            {"producer": PRODUCER_URL, "schemaURL": SCHEMA_URL},
        ),
        type=RunEventType.REQUESTED,
        time=now(as_str=True),
        inputs=run_spec.inputs,
    )
    mdb.run_events.insert_one(event.model_dump())
    return run_id


def _add_run_started_event(run_id: str, mdb: MongoDatabase):
    requested: RunEvent = RunEvent(
        **raise404_if_none(
            mdb.run_events.find_one(
                {"run.id": run_id, "type": "REQUESTED"}, sort=[("time", -1)]
            )
        )
    )
    mdb.run_events.insert_one(
        RunEvent(
            producer=PRODUCER_URL,
            schemaURL=SCHEMA_URL,
            run=requested.run,
            job=requested.job,
            type=RunEventType.STARTED,
            time=now(as_str=True),
        ).model_dump()
    )
    return run_id


def _add_run_fail_event(run_id: str, mdb: MongoDatabase):
    requested: RunEvent = RunEvent(
        **raise404_if_none(
            mdb.run_events.find_one(
                {"run.id": run_id, "type": "REQUESTED"}, sort=[("time", -1)]
            )
        )
    )
    mdb.run_events.insert_one(
        RunEvent(
            producer=PRODUCER_URL,
            schemaURL=SCHEMA_URL,
            run=requested.run,
            job=requested.job,
            type=RunEventType.FAIL,
            time=now(as_str=True),
        ).model_dump()
    )
    return run_id


def _add_run_complete_event(run_id: str, mdb: MongoDatabase, outputs: List[str]):
    started: RunEvent = RunEvent(
        **raise404_if_none(
            mdb.run_events.find_one(
                {"run.id": run_id, "type": "STARTED"}, sort=[("time", -1)]
            )
        )
    )
    mdb.run_events.insert_one(
        RunEvent(
            producer=PRODUCER_URL,
            schemaURL=SCHEMA_URL,
            run=started.run,
            job=started.job,
            type=RunEventType.COMPLETE,
            time=now(as_str=True),
            outputs=outputs,
        ).model_dump()
    )
    return run_id
