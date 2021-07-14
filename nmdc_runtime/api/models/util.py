from typing import TypeVar, List, Optional, Generic

from pydantic import BaseModel
from pydantic.generics import GenericModel

ResultT = TypeVar("ResultT")


class ListResponse(GenericModel, Generic[ResultT]):
    resources: List[ResultT]
    next_page_token: Optional[str]


class ListRequest(BaseModel):
    filter: Optional[str]
    max_page_size: Optional[int] = 20
    page_token: Optional[str]
