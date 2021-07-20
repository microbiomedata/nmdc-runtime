import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel

from nmdc_runtime.api.models.operation import Metadata as OperationMetadata
from nmdc_runtime.api.models.workflow import Workflow


class JobBase(BaseModel):
    workflow: Workflow
    name: Optional[str]
    description: Optional[str]


class Job(JobBase):
    id: str
    created_at: Optional[datetime.datetime]
    config: Dict[str, Any]


class JobExecution(BaseModel):
    id: str
    job: Job


class JobOperationMetadata(OperationMetadata):
    job: Job
    site_id: str
