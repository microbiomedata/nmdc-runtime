import pytest

from typing import Any, Dict

from components.job.job.schema import (
    CreateJob,
    UpdateJob,
    JobQuery,
    JobOut,
    PydanticVersion,
)
from components.job.job import JobService

JOB_OUT = JobOut.parse_obj({"name": "beans", "version": "1.2.3"})


class JobQueryDummy(JobQuery):
    async def create_job(self, job) -> JobOut:
        return JOB_OUT

    async def update_job(self, job_update) -> JobOut:
        return JOB_OUT


@pytest.fixture
def job_out() -> JobOut:
    return JOB_OUT


@pytest.fixture
def job_schema() -> Dict[str, Any]:
    return {
        "name": "beans",
        "version": "1.2.3",
        "wdl_repo": "https://fake.news",
        "inputs": ["/tmp/"],
    }


# @pytest.fixture
# def job_update_schema() -> UpdateJob:
#     return UpdateJob()


class TestJobService:
    @pytest.mark.asyncio
    async def test_job_create_valid(
        self, job_out: JobOut, job_schema: CreateJob
    ) -> None:
        job_service = JobService(JobQueryDummy())

        result = await job_service.create_job(job_schema)
        print(result)
        assert result["name"] == "beans"
        assert result["version"] == "1.2.3"
