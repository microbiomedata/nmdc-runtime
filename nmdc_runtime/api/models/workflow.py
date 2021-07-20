import datetime
from typing import Optional, List

from pydantic import BaseModel


class WorkflowBase(BaseModel):
    name: Optional[str]
    description: Optional[str]
    capability_ids: Optional[List[str]]


class Workflow(WorkflowBase):
    id: str
    created_at: Optional[datetime.datetime]
