from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime

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

class JGISample(BaseModel):
    """
    Represents a JGI Sample for workflow file staging.
    """
    ap_gold_id: str = Field(
        ..., description="AP Gold ID", examples=["Some AP Gold ID"]
    )
    study_id: str = Field(
        ..., description="Study ID", examples=["Some Study ID"]
    )
    its_ap_id: int = Field(
        ..., description="ITS AP ID", examples=[12345]
    )
    project_name: str = Field(
        ..., description="Project Name", examples=["Some Project Name"]
    )
    biosample_id: str = Field(
        ..., description="Biosample ID", examples=["Some Biosample ID"]
    )
    seq_id: str = Field(
        ..., description="Sequence ID", examples=["Some Sequence ID"]
    )
    file_name: str = Field(
        ..., description="File Name", examples=["some_file.fastq"]
    )
    file_status: str = Field(
        ..., description="File Status", examples=["Some File Status"]
    )
    file_size: int = Field(
        ..., description="File Size", examples=[123456]
    )
    jdp_file_id: str = Field(
        ..., description="JDP File ID", examples=["Some JDP File ID"]
    )
    md5sum: str = Field(
        None, description="MD5 Sum", examples=["Some MD5 Sum"]
    )
    analysis_project_id: str = Field(
        ..., description="Analysis Project ID", examples=["Some Analysis Project ID"]
    )
    create_date: datetime = Field(default_factory=datetime.now, description="Creation Date", examples=["2023-01-01T00:00:00Z"])
    update_date: datetime = Field(None, description="Update Date", examples=["2023-01-01T00:00:00Z"])
    request_id: int = Field(default=0, description="Request ID", examples=[1])
