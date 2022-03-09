from fastapi import HTTPException, Depends
from uuid import UUID, uuid4
from datetime import datetime

from nmdc_runtime.models.job import (
    JobCreate,
    JobStatus,
    JobTimestamps,
    JobInDB,
    JobData,
)
from nmdc_runtime.db import DatabaseManager, get_database

# Same as file name /nmdc_runtime/search_index_definitions/jos.json
full_text_index_name = "jobs"


# async def get(*, job_id: UUID, db: DatabaseManager = Depends(get_database)):
#     if (job := await db["jobs"].find_one({"job_id": job_id})) is not None:
#         return job

#     raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


async def job_create(
    doc_in: JobCreate, db: DatabaseManager = Depends(get_database)
):
    doc = JobInDB(**doc_in)
    job = db.add_job(doc)


return job
