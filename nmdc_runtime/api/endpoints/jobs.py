import json
from typing import Optional

import pymongo
from fastapi import APIRouter, Depends

from nmdc_runtime.api.core.util import (
    raise404_if_none,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import list_resources, _claim_job
from nmdc_runtime.api.models.job import Job
from nmdc_runtime.api.models.operation import Operation, MetadataT
from nmdc_runtime.api.models.site import (
    Site,
    maybe_get_current_client_site,
    get_current_client_site,
)
from nmdc_runtime.api.models.util import ListRequest, ListResponse, ResultT

router = APIRouter()


@router.get(
    "/jobs", response_model=ListResponse[Job], response_model_exclude_unset=True
)
def list_jobs(
    req: ListRequest = Depends(),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    maybe_site: Optional[Site] = Depends(maybe_get_current_client_site),
):
    """List pre-configured workflow jobs.

    If authenticated as a site client, `req.filter` defaults to fetch unclaimed jobs
    that are claimable by the site client. This default can be overridden to view all jobs
    by explicitly passing a `req.filter` of `{}`.
    """
    if isinstance(maybe_site, Site) and req.filter is None:
        req.filter = json.dumps({"claims.site_id": {"$ne": maybe_site.id}})
    return list_resources(req, mdb, "jobs")


@router.get("/jobs/{job_id}", response_model=Job, response_model_exclude_unset=True)
def get_job_info(
    job_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.jobs.find_one({"id": job_id}))


@router.post("/jobs/{job_id}:claim", response_model=Operation[ResultT, MetadataT])
def claim_job(
    job_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    return _claim_job(job_id, mdb, site)


@router.get(
    "/jobs/{job_id}/executions",
    description=(
        "A sub-resource of a job resource, the result of a successful run of that job. "
        "An execution resource may be retrieved by any site; however, it may be created "
        "and updated only by the site that ran its job."
    ),
)
def list_job_executions():
    # TODO
    pass


@router.get("/jobs/{job_id}/executions/{exec_id}")
def get_job_execution():
    # TODO
    pass
