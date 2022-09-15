from datetime import datetime
from typing import Dict, List, Union

from beanie import Document, Indexed
from pydantic import DirectoryPath, HttpUrl, ValidationError

from .schema import CreateJob, PydanticVersion, UpdateJob, JobOut, JobQuery


class Job(Document, JobOut):
    name: Indexed(str, unique=True)
    updated_at: Indexed(datetime) = datetime.now()
    created_at: Indexed(datetime) = datetime.now()


class JobQueryInDb(JobQuery):
    async def update_job(self, update: UpdateJob) -> JobOut:
        """Update a runtime workflow job"""
        try:
            await job.set(update.dict())
            return job
        except ValidationError as e:
            raise ValidationError from e

    async def create_job(self, job: CreateJob) -> JobOut:
        """Create a new runtime workflow job

        Parameters
        ----------
        job : CreateJob
        The job initializing data

        Returns
        -------
        JobOut
        an object representing a newly minted job
        """
        now = datetime.now()
        try:
            new_job = Job.parse_obj(job)
            await new_job.insert()
            return new_job
        except ValidationError as e:
            raise ValidationError from e
