from abc import abstractmethod
from typing import List
from uuid import UUID

from nmdc_runtime.models.job import JobInDB, JobCreate


class DatabaseManager(object):
    @property
    def client(self):
        raise NotImplementedError

    @property
    def db(self):
        raise NotImplementedError

    @abstractmethod
    async def connect_to_database(self, path: str):
        pass

    @abstractmethod
    async def close_database_connection(self):
        pass

    @abstractmethod
    async def get_jobs(self) -> List[JobInDB]:
        pass

    @abstractmethod
    async def get_job(self, job_id: UUID) -> JobInDB:
        pass

    @abstractmethod
    async def add_job(self, job: JobCreate) -> None:
        pass

    @abstractmethod
    async def update_job(self, job_id: UUID, job: JobInDB) -> None:
        pass

    @abstractmethod
    async def delete_job(self, job_id: UUID) -> None:
        pass
