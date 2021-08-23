import datetime
from typing import Optional, List

from pydantic import BaseModel

from nmdc_runtime.api.models.object import DrsObject


class ObjectTypeBase(BaseModel):
    name: Optional[str]
    description: Optional[str]


class ObjectType(ObjectTypeBase):
    id: str
    created_at: datetime.datetime


class DrsObjectWithTypes(DrsObject):
    types: Optional[List[str]]
