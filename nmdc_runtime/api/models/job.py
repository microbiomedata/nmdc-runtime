import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel

from nmdc_runtime.api.models.operation import Metadata as OperationMetadata
from nmdc_runtime.api.models.workflow import Workflow


class JobBase(BaseModel):
    workflow: Workflow
    name: Optional[str] = None
    description: Optional[str] = None


class JobClaim(BaseModel):
    op_id: str
    site_id: str


class Job(JobBase):
    id: str
    created_at: Optional[datetime.datetime] = None
    config: Dict[str, Any]
    claims: List[JobClaim] = []


class JobExecution(BaseModel):
    id: str
    job: Job


class JobOperationMetadata(OperationMetadata):
    job: Job
    site_id: str
