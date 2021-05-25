import datetime
from typing import Optional

from pydantic import BaseModel


class ObjectTypeBase(BaseModel):
    name: Optional[str]
    description: Optional[str]


class ObjectType(ObjectTypeBase):
    id: str
    created_at: datetime.datetime
