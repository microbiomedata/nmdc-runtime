import json
from typing import Optional, Annotated

from pymongo.database import Database
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from pymongo.errors import ConnectionFailure, OperationFailure
from starlette import status

from nmdc_runtime.api.core.util import (
    raise404_if_none,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import list_resources, _claim_job
from nmdc_runtime.api.models.job import Job, JobClaim
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
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
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
    mdb: Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.jobs.find_one({"id": job_id}))


@router.post("/jobs/{job_id}:claim", response_model=Operation[ResultT, MetadataT])
def claim_job(
    job_id: str,
    mdb: Database = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    return _claim_job(job_id, mdb, site)


@router.post(
    "/jobs/{job_id}:release", response_model=Job, response_model_exclude_unset=True
)
def release_job(
    job_id: Annotated[
        str,
        Path(
            title="Job ID",
            description="The `id` of the job.\n\n_Example_: `nmdc:f81d4fae-7dec-11d0-a765-00a0c91e6bf6`",
            examples=["nmdc:f81d4fae-7dec-11d0-a765-00a0c91e6bf6"],
        ),
    ],
    mdb: Database = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """Cancel all operations registered as claims by `site` for job `job_id`.

    Return the updated job document, reflecting that all of this site's claims are cancelled.
    """
    job = Job(**raise404_if_none(mdb.jobs.find_one({"id": job_id})))
    active_job_claims_by_this_site = list(
        mdb.operations.find(
            {
                "metadata.job.id": job_id,
                "metadata.site_id": site.id,
                "done": False,
            },
            ["id"],
        )
    )
    job_claims_by_this_site_post_release = [
        JobClaim(op_id=claim["id"], site_id=site.id, done=True, cancelled=True)
        for claim in active_job_claims_by_this_site
    ]
    job_claims_not_by_this_site = [
        claim for claim in job.claims if (claim.site_id != site.id)
    ]

    # Execute MongoDB transaction to ensure atomic change of job document plus relevant set of operations documents.
    def transactional_update(session):
        mdb.operations.update_many(
            {"id": {"$in": [claim["id"] for claim in active_job_claims_by_this_site]}},
            {"$set": {"metadata.cancelled": True, "metadata.done": True}},
            session=session,
        )
        job_claim_subdocuments_post_release = [
            claim.model_dump(exclude_unset=True)
            for claim in (
                job_claims_not_by_this_site + job_claims_by_this_site_post_release
            )
        ]
        mdb.jobs.update_one(
            {"id": job_id},
            {"$set": {"claims": job_claim_subdocuments_post_release}},
            session=session,
        )

    try:
        with mdb.client.start_session() as session:
            with session.start_transaction():
                transactional_update(session)
    except (ConnectionFailure, OperationFailure) as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Transaction failed: {e}",
        )
    return mdb.jobs.find_one({"id": job_id})
