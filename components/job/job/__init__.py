from typing import Any, Dict

from components.job.job.schema import JobQuery, CreateJob, UpdateJob
from components.job.job.store import Job, JobQueryInDb


def get_beanie_document():
    return [Job]


class JobService:
    def __init__(self, job_queries: JobQuery = JobQueryInDb()) -> None:
        self.__job_queries = job_queries

    async def create_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """A function to create a new workflow job

        :param job: dictionary of fields for job creation
        :type job: Dict[str, Any]

        :return stuff: stuff
        :type stuff: Dict[str, Any]
        """
        new_job = CreateJob.parse_obj(job)
        result = await self.__job_queries.create_job(new_job)
        return result.dict()

    async def update_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        job_update = UpdateJob.parse_obj(job)
        result = await self.__job_queries.update_job(job_update)
        return result.dict()

    async def fetch_jobs(self, job_query: Dict[str, Any]) -> Dict[str, Any]:
        pass
