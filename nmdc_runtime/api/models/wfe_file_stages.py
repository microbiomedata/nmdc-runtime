from pydantic import BaseModel, Field
from enum import Enum


class GlobusTaskStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


class GlobusTask(BaseModel):
    """
    Represents a Globus file transfer configuration.
    """

    task_id: str = Field(
        ..., description="ID from Globus of the task", examples=["Some task id"]
    )
    task_status: GlobusTaskStatus = Field(
        ..., description="Status of the globus task.", examples=["Some status"]
    )
