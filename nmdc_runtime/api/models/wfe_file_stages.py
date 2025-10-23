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
    task_status: str = Field(
        ..., description="Status of the Globus task.", examples=["Some status"]
    )


class SequencingProject(BaseModel):
    """
    Represents a sequencing project with its associated metadata.
    """

    project_name: str = Field(
        ...,
        description="Name of the sequencing project",
        examples=["Human Genome Project"],
    )
    description: str = Field(
        ...,
        description="Detailed description of the sequencing project",
        examples=["A project to sequence the human genome."],
    )
    proposal_id: str = Field(..., description="JGI proposal ID", examples=["503568"])

    nmdc_study_id: str = Field(
        ..., description="NMDC study ID", examples=["nmdc:sty-11-28tm5d36"]
    )
