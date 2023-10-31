import datetime
from typing import Optional

from pydantic import BaseModel


class CapabilityBase(BaseModel):
    name: Optional[str]
    description: Optional[str]


class Capability(CapabilityBase):
    id: str
    created_at: datetime.datetime
