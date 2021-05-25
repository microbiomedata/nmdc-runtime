import datetime
from typing import Optional, Any

from pydantic import BaseModel, root_validator


class QueryBase(BaseModel):
    name: Optional[str]
    description: Optional[str]
    save: bool = False
    query: Any


class Query(QueryBase):
    id: str
    created_at: datetime.datetime
    last_ran: datetime.datetime
    result: Optional[Any]
    error: Optional[Any]

    @root_validator
    def result_xor_error(cls, values):
        result, error = values.get("result"), values.get("error")
        if result is None and error is None:
            raise ValueError("At least one of result and error must be provided.")
        if result is not None and error is not None:
            raise ValueError("Only one of result or error must be provided.")
        return values
