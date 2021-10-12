import json
from typing import Optional

import pymongo
from fastapi import APIRouter, Depends, HTTPException
from starlette import status

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import (
    raise404_if_none,
    expiry_dt_from_now,
    dotted_path_for,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.job import Job, JobOperationMetadata, JobClaim
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
    job_doc = raise404_if_none(mdb.jobs.find_one({"id": job_id}))
    job = Job(**job_doc)
    # check that site satisfies the job's workflow's required capabilities.
    capabilities_required = job.workflow.capability_ids or []
    for cid in capabilities_required:
        if cid not in site.capability_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"client site does not have capability {cid} required to claim job",
            )

    # For now, allow site to claim same job multiple times,
    # to re-submit results given same job input config.
    job_op_for_site = mdb.operations.find_one(
        {"metadata.job.id": job.id, "metadata.site_id": site.id}
    )
    if job_op_for_site is not None:
        # raise HTTPException(
        #     status_code=status.HTTP_409_CONFLICT,
        #     detail={
        #         "msg": (
        #             f"client site already claimed job -- "
        #             f"see operation {job_op_for_site['id']}"
        #         ),
        #         "id": job_op_for_site["id"],
        #     },
        # )
        pass

    op_id = generate_one_id(mdb, "op")
    job.claims = (job.claims or []) + [JobClaim(op_id=op_id, site_id=site.id)]
    op = Operation[ResultT, JobOperationMetadata](
        **{
            "id": op_id,
            "expire_time": expiry_dt_from_now(days=30),
            "metadata": {
                "job": Job(
                    **{
                        "id": job.id,
                        "workflow": {"id": job.workflow.id},
                        "config": job.config,
                    }
                ).dict(exclude_unset=True),
                "site_id": site.id,
                "model": dotted_path_for(JobOperationMetadata),
            },
        }
    )
    mdb.operations.insert_one(op.dict())
    mdb.jobs.replace_one({"id": job.id}, job.dict(exclude_unset=True))
    return op.dict(exclude_unset=True)


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
