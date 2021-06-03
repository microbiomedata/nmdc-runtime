import datetime
from typing import Optional

from pydantic import BaseModel


class TriggerBase(BaseModel):
    object_type_id: str
    workflow_id: str


class Trigger(TriggerBase):
    id: str
    created_at: datetime.datetime
