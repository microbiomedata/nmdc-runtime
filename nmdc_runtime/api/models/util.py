from typing import TypeVar, List, Optional, Generic

from pydantic import BaseModel, root_validator, conint
from pydantic.generics import GenericModel

ResultT = TypeVar("ResultT")


class ListResponse(GenericModel, Generic[ResultT]):
    resources: List[ResultT]
    next_page_token: Optional[str]


class ListRequest(BaseModel):
    filter: Optional[str]
    max_page_size: Optional[int] = 20
    page_token: Optional[str]


PerPageRange = conint(gt=0, le=200)


class FindRequest(BaseModel):
    filter: Optional[str]
    search: Optional[str]
    sort: Optional[str]
    page: Optional[int]
    per_page: Optional[PerPageRange] = 25
    cursor: Optional[str]
    group_by: Optional[str]

    @root_validator(pre=True)
    def set_page_if_cursor_unset(cls, values):
        page, cursor = values.get("page"), values.get("cursor")
        if page is not None and cursor is not None:
            raise ValueError("cannot use cursor- and page-based pagination together")
        if page is None and cursor is None:
            values["page"] = 1
        return values


class FindResponse(BaseModel):
    meta: dict
    results: List[dict]
    group_by: List[dict]
