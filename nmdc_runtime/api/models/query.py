import datetime
from typing import Optional, Any, Dict, List, Union

from pydantic import (
    BaseModel,
    root_validator,
    conint,
    PositiveInt,
    NonNegativeInt,
)

Document = Dict[str, Any]

OneOrZero = conint(ge=0, le=1)
One = conint(ge=1, le=1)
MinusOne = conint(ge=-1, le=-1)
OneOrMinusOne = Union[One, MinusOne]


# TODO need to figure out how to opt out of cursor sessions, or else get the cursor session id back.
#   Okay, looks like I need to explicitly model and manage server sessions:
#   https://docs.mongodb.com/manual/reference/server-sessions/
#   Ugh. Fine. I can do this.


class CommandBase(BaseModel):
    comment: Optional[Any]


class CountCommand(CommandBase):
    count: str
    query: Optional[Document]


class FindCommand(CommandBase):
    find: str
    filter: Optional[Document]
    projection: Optional[Dict[str, OneOrZero]]
    allowPartialResults: Optional[bool] = True
    batchSize: Optional[PositiveInt] = 101
    sort: Optional[Dict[str, OneOrMinusOne]]
    limit: Optional[NonNegativeInt]


class CommandResponse(BaseModel):
    ok: OneOrZero


class CountCommandResponse(CommandResponse):
    n: NonNegativeInt


class FindCommandResponseCursor(BaseModel):
    firstBatch: List[Document]
    partialResultsReturned: Optional[bool]
    id: Optional[int]
    ns: str


class FindCommandResponse(CommandResponse):
    cursor: FindCommandResponseCursor


class DeleteCommandDelete(BaseModel):
    q: Document
    limit: OneOrZero
    hint: Optional[Dict[str, OneOrMinusOne]]


class DeleteCommand(CommandBase):
    delete: str
    deletes: List[DeleteCommandDelete]


class DeleteCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    writeErrors: Optional[List[Document]]


class GetMoreCommand(CommandBase):
    getMore: int
    collection: str
    batchSize: Optional[PositiveInt]


class GetMoreCommandResponseCursor(BaseModel):
    nextBatch: List[Document]
    partialResultsReturned: Optional[bool]
    id: Optional[int]
    ns: str


class GetMoreCommandResponse(CommandResponse):
    cursor: GetMoreCommandResponseCursor


QueryCmd = Union[CountCommand, FindCommand, GetMoreCommand, DeleteCommand]

QueryResponseOptions = Union[
    CountCommandResponse,
    FindCommandResponse,
    GetMoreCommandResponse,
    DeleteCommandResponse,
]


def command_response_for(type_):
    d = {
        CountCommand: CountCommandResponse,
        FindCommand: FindCommandResponse,
        GetMoreCommand: GetMoreCommandResponse,
        DeleteCommand: DeleteCommandResponse,
    }
    return d.get(type_)


class Query(BaseModel):
    id: str
    saved_at: datetime.datetime
    cmd: QueryCmd


class QueryRun(BaseModel):
    qid: str
    ran_at: datetime.datetime
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
