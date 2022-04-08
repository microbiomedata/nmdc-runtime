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


class FindRequest(BaseModel):
    filter: Optional[str]
    search: Optional[str]
    sort: Optional[str]
    page: Optional[int] = 1
    per_page: Optional[int] = 25
    cursor: Optional[str]
    group_by: Optional[str]


class FindResponse(BaseModel):
    meta: dict
    results: List[dict]
    group_by: List[dict]
