from bson import ObjectId
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from uuid import UUID


from nmdc_runtime.models.mongo import PyObjectId


class JobData(BaseModel):
    parameters: Optional[Dict[str, str]]
    inputs: List[str]
    outputs: List[str]


class JobStatus(str, Enum):
    waiting = ("waiting",)
    working = ("working",)
    done = ("done",)
    cancelled = "cancelled"


class JobTimestamps(BaseModel):
    created_at: Optional[datetime]
    started_at: Optional[datetime]
    done_at: Optional[datetime]


# Shared properties of all Jobs
class JobID(BaseModel):
    job_id: UUID


class JobBase(BaseModel):
    """
    A J-O-B
    """

    job_id: JobID = Field(...)
    status: JobStatus = Field(...)
    timestamps: JobTimestamps = Field(...)


# Properties to receive on item creation
class JobCreate(JobBase):
    status: JobStatus = Field(...)
    data: JobData = Field(...)


# Properties to receive on item update
class JobUpdate(JobBase):
    pass


# Properties to return to client
class Job(JobBase):
    pass


# Properties stored in MongoDB
class JobInDB(JobBase):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")

    class Config:
        allow_population_by_field_name = True
        json_encoders = {ObjectId: str}
