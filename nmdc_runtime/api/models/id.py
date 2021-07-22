import datetime
from typing import Optional, List

from pydantic import BaseModel

from nmdc_runtime.api.core.idgen import Base32Id, IdShoulder


class IdRequest(BaseModel):
    shoulder: Optional[IdShoulder]
    ns: Optional[str]


class Id(BaseModel):
    id: Base32Id
    ns: Optional[str]
