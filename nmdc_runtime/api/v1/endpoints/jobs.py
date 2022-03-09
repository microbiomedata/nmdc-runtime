from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks

from nmdc_runtime.db import get_database
from nmdc_runtime.crud.gff import gff_to_json, add_gffs
from nmdc_runtime.db.database_manager import DatabaseManager
from nmdc_runtime.models.job import JobData, Job, JobID


router = APIRouter()


@router.post("", status_code=201, response_model=JobID)
async def create_job(
    data_in: JobData, background_tasks: BackgroundTasks
) -> JobID:
    """
    Create a new job.
    """
    result_id = gff_to_json(data_in.inputs)
    if result_id is not UUID:
        raise HTTPException(
            status_code=500,
            detail="ID for job not created.",
            headers={"X-Error": "Whoopsies."},
        )
    background_tasks.add_task(add_gffs, result_id)
    return JobID(job_id=result_id)


@router.get("/{job_id}", status_code=200, response_model=Job)
async def get_job(job_id: UUID, db: DatabaseManager = Depends(get_database)):
    return await db.get_job(job_id)
