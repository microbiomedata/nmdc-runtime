import datetime
from typing import Optional

from pydantic import BaseModel


class WorkflowBase(BaseModel):
    name: Optional[str]
    description: Optional[str]


class Workflow(WorkflowBase):
    id: str
    created_at: datetime.datetime
