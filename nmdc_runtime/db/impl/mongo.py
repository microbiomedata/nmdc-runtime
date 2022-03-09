import logging
from typing import List
from uuid import UUID
from motor.motor_asyncio import AsyncIOMotorClient
from motor.core import AgnosticClient, AgnosticDatabase

from nmdc_runtime.db.database_manager import DatabaseManager
from nmdc_runtime.models.job import JobInDB, JobCreate


class MongoManager(DatabaseManager):
    client: AgnosticClient
    db: AgnosticDatabase

    async def connect_to_database(self, path: str) -> None:
        logging.info("Connecting to Mongo")
        self.client = AsyncIOMotorClient(path, maxPoolSize=10, minPoolSize=10)
        self.db = self.client.main_db
        logging.info("Connected to MongoDB")

    async def close_database_connection(self) -> None:
        logging.info("Closing connection with MongoDB.")
        self.client.close()
        logging.info("Closed connection with MongoDB.")

    async def get_jobs(self) -> List[JobInDB]:
        jobs_list = []
        jobs_q = self.db.jobs.find({"status": {"$eq": "waiting"}})
        for job in await jobs_q.to_list(length=1000):
            jobs_list.append(job)
        return jobs_list

    async def get_job(self, job_id: UUID) -> JobInDB:
        try:
            job_q = await self.db.jobs.find_one({"job_id": job_id})
            return JobInDB(**job_q, id=job_q["_id"])
        except Exception:
            raise Exception("exception")

    async def update_job(self, job_id: UUID, job: JobInDB) -> None:
        await self.db.jobs.update_one(
            {"job_id": job_id}, {"$set": job.dict(exclude={"id"})}
        )

    async def add_job(self, job: JobCreate) -> None:
        await self.db.jobs.insert_one(job.dict(exclude={"id"}))

    async def delete_job(self):
        pass
