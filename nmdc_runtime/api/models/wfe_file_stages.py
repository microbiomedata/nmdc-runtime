from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
import datetime


class GlobusTaskStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


class JDPFileStatus(str, Enum):
    RESTORED = "RESTORED"
    PURGED = "PURGED"
    READY = "READY"
    EXPIRED = "EXPIRED"


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
    Represents a JGI Sample for workflow file staging. Information from JDP, Gold, and Globus is gathered on these records.
    """

    jdp_file_id: str = Field(
        ...,
        description="JGI Data Portal File ID",
        examples=["6011bc6e117e5d4b9d2b2073"],
    )
    ap_gold_id: str = Field(
        ..., description="Gold Analysis Project ID", examples=["Ga0307276"]
    )
    gold_study_id: str = Field(..., description="Gold Study ID", examples=["Gs0135149"])
    its_ap_id: str = Field(
        ..., description="ITS Analysis Project ID from the JDP", examples=["1196479.0"]
    )
    sequencing_project_name: str = Field(
        ...,
        description="Sequencing project name. This relates to a record in the `/wf_staging_file/sequencing_project` endpoints.",
        examples=["Some Project Name"],
    )
    gold_biosample_id: str = Field(
        ..., description="Gold Biosample ID", examples=["Gb0191643"]
    )
    gold_seq_id: str = Field(..., description="Gold Sequence ID", examples=["1196479"])
    file_name: str = Field(..., description="File Name", examples=["filename.tar.gz"])
    jdp_file_status: str = Field(
        ...,
        description="File staging status. Grabbed from the JDP file restoration endpoint.",
        examples=["RESTORED"],
    )
    globus_file_status: str = Field(
        ...,
        description="File staging status. Recieved from Globus when the file state is queried.",
        examples=["ACTIVE"],
    )
    jdp_file_size: int = Field(
        ..., description="File size in bytes from JDP.", examples=[123456]
    )
    md5sum: Optional[str] = Field(
        None, description="MD5 Sum", examples=["D43F2404CA13E22594E5C8B04D3BBB81"]
    )
    jgi_ap_id: str = Field(
        ..., description="JGI Analysis Project ID", examples=["1196479"]
    )
    create_date: datetime.datetime = Field(
        ..., description="Creation Date", examples=["2023-01-01T00:00:00Z"]
    )
    update_date: Optional[datetime.datetime] = Field(
        None, description="Update Date", examples=["2023-01-01T00:00:00Z"]
    )
    request_id: int = Field(
        ...,
        description="Request ID from the JGI data portal after a request to have the files restored from tape is submitted.",
        examples=[1],
    )


class SequencingProject(BaseModel):
    """
    Represents a JGI sequencing project with its associated metadata.
    """

    sequencing_project_name: str = Field(
        ...,
        description="Name of the sequencing project that we can refer to while staging files.",
        examples=["Human Genome Project"],
    )
    sequencing_project_description: str = Field(
        ...,
        description="Detailed description of the sequencing project",
        examples=["A project to sequence the human genome."],
    )
    jgi_proposal_id: str = Field(
        ..., description="JGI proposal ID", examples=["503568"]
    )

    nmdc_study_id: str = Field(
        ..., description="NMDC study ID", examples=["nmdc:sty-11-28tm5d36"]
    )
