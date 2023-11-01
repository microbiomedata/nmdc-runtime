import datetime
from typing import Optional, List

from pydantic import BaseModel


class WorkflowBase(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    capability_ids: Optional[List[str]] = None


class Workflow(WorkflowBase):
    id: str
    created_at: Optional[datetime.datetime] = None
