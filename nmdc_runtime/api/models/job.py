import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Field

from nmdc_runtime.api.models.operation import Metadata as OperationMetadata
from nmdc_runtime.api.models.workflow import Workflow


class JobBase(BaseModel):
    workflow: Workflow
    name: Optional[str] = Field(
        None, description="Name of the job", examples=["Some job"]
    )
    description: Optional[str] = Field(
        None, description="Description of the job", examples=["Some description"]
    )


class JobClaim(BaseModel):
    op_id: str
    site_id: str
    done: Optional[bool] = None
    cancelled: Optional[bool] = None


class Job(JobBase):
    id: str
    created_at: Optional[datetime.datetime] = None
    config: Dict[str, Any]
    claims: List[JobClaim] = []


class JobIn(JobBase):
    """Payload of an HTTP request to create a `Job`."""

    # Consider forbidding extra fields (once the workflow automation developers have
    # updated the client code accordingly).
    # See: https://docs.pydantic.dev/latest/api/config/#pydantic.config.ConfigDict.extra
    ##model_config = ConfigDict(extra="forbid")

    config: Dict[str, Any] = Field(
        ..., description="Configuration of the associated workflow", examples=[{}]
    )
    claims: List[JobClaim] = Field(
        default_factory=list, description="Claims of the job", examples=[[]]
    )


class JobExecution(BaseModel):
    id: str
    job: Job


class JobOperationMetadata(OperationMetadata):
    job: Job
    site_id: str
