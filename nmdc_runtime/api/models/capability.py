import datetime
from typing import Optional

from pydantic import BaseModel


class CapabilityBase(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class Capability(CapabilityBase):
    id: str
    created_at: datetime.datetime
