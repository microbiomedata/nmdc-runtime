import json
import uuid
from datetime import datetime, timezone
from typing import Optional, Annotated

from pymongo.database import Database
from fastapi import APIRouter, Depends, Query, HTTPException, Path, Body
from pymongo.errors import ConnectionFailure, OperationFailure
from starlette import status

from nmdc_runtime.api.core.util import (
    raise404_if_none,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.endpoints.util import list_resources, _claim_job, check_action_permitted
from nmdc_runtime.api.models.job import Job, JobClaim
from nmdc_runtime.api.models.user import User, get_current_active_user
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

@router.post("/jobs", response_model=Job, status_code=status.HTTP_201_CREATED)
def create_job(
    job_data: dict = Body(...),
    mdb: Database = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """
    Create a new workflow job record.
    
    This endpoint creates a new job that defines a workflow execution request. The job includes:
    
    - **Workflow Information**: Identifies the specific workflow to be executed
    - **Configuration**: Detailed parameters including input/output specifications, repository information, and execution parameters
    - **Claims**: Optional site assignments for job execution
    
    The system will automatically generate a unique job ID and creation timestamp if not provided.
    
    **Required Permissions**: Only users with specific job creation permissions can use this endpoint.
    
    **Example request bodies:**
    - Minimal job creation:
    
    ```json
    {
      "workflow": {
        "id": "wf id"
      },
      "created_at": "date",
      "config": {
        "git_repo": "https://github.com/",
        "release": "v1.1.0",
        "wdl": "annotation_full.wdl",
        "activity_id": "nmdc:wfmgan-123",
        "activity_set": "metagenome_annotation_activity_set",
        "was_informed_by": [
          "nmdc:omprc-11-123"
        ],
        "trigger_activity": "nmdc:wfmgas-11-123",
        "iteration": 2,
        "input_prefix": "annotation",
        "inputs": {
          "input_file": "file link",
          "imgap_project_id": "project id",
          "proj": "nmdc:wfmgan-11-123"
        },
        "input_data_objects": [
          {
            "id": "nmdc:dobj-11-123",
            "name": "data object name",
            "description": "data object description",
            "url": "https://data.microbiomedata.org/data/nmdc:omprc-11-123/nmdc:wfmgas-11-123/nmdc_wfmgas-11-123_contigs.fna",
            "md5_checksum": "123",
            "file_size_bytes": 123,
            "data_object_type": "Assembly Contigs"
          }
        ],
        "activity": {
          "name": "Metagenome Annotation Analysis Activity for {id}",
          "type": "nmdc:MetagenomeAnnotationActivity"
        },
        "outputs": [
          {
            "output": "output",
            "data_object_type": "Annotation Amino Acid FASTA",
            "description": "description of output",
            "name": "output name",
            "id": "nmdc:dobj-123"
          }
        ]
      },
      "claims": [
        {
          "op_id": "nmdc:123",
          "site_id": "NERSC"
        }
      ]
    }
    ```
   

    """
    _ = site  # must be authenticated
    
    
    # Generate a unique ID for the job
    job_data["id"] = generate_one_id(mdb, "jobs")
    
    # Set created_at timestamp if not provided
    if "created_at" not in job_data:
        job_data["created_at"] = datetime.now(timezone.utc)
    
    # Validate the job data structure
    try:
        job = Job(**job_data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid job data: {str(e)}"
        )
    # convert back to dict after validation
    job_dict = job.model_dump(exclude_unset=True)
    
    # Insert the job into the database
    try:
        result = mdb.jobs.insert_one(job_dict)
        if not result.inserted_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create job"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
    
    # Return the created job
    return job

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


@router.post("/jobs/{job_id}:release")
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
) -> Optional[Job]:
    r"""
    Release the specified job.

    Releasing a job cancels all the unfinished operations (of that job)
    claimed by the `site` associated with the logged-in site client.

    Return the updated job, reflecting that the aforementioned operations have been cancelled.
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

    # Return the updated `jobs` document.
    #
    # TODO: Consider retrieving the document within the transaction
    #       to ensure it still exists.
    #
    updated_job = mdb.jobs.find_one({"id": job_id})
    if updated_job is None:
        # Note: We return `None` in this case because that's what the
        #       endpoint originally did in this case, and we don't want
        #       to introduce a breaking change as part of this refactor.
        return None
    else:
        return Job(**updated_job)
