from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Depends, Response, status
from pymongo.errors import DuplicateKeyError

from components.job.job import JobService, JobOut, CreateJob, UpdateJob

router = APIRouter(prefix="/workflow-jobs", tags=["workflow_jobs"])


@router.post("", response_model=JobOut, status_code=status.HTTP_201_CREATED)
async def create(job: CreateJob) -> Dict[str, Any]:
    try:
        job_service = JobService()
        new_job = await job_service.create_job(job)
        return JobOut.parse_obj(new_job)
    except DuplicateKeyError as e:
        raise HTTPException(
            status_code=409,
            detail=f"workflow job name '{job.name}' is already in use",
        )


@router.patch(
    "/{job_name}", response_model=JobOut, status_code=status.HTTP_200_OK
)
async def read(job_name: str, job_update: UpdateJob) -> Dict[str, Any]:
    try:
        job_service = JobService()
        result = await job_service.update_job(job_update.dict())
        return JobOut.parse_obj(result)
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404)
