import datetime
from typing import Optional, Any, Dict, List, Union

from pydantic import (
    model_validator,
    Field,
    BaseModel,
    PositiveInt,
    NonNegativeInt,
)
from typing_extensions import Annotated

Document = Dict[str, Any]

OneOrZero = Annotated[int, Field(ge=0, le=1)]
One = Annotated[int, Field(ge=1, le=1)]
MinusOne = Annotated[int, Field(ge=-1, le=-1)]
OneOrMinusOne = Union[One, MinusOne]


class CommandBase(BaseModel):
    comment: Optional[Any] = None


class CollStatsCommand(CommandBase):
    collStats: str
    scale: Optional[int] = 1


class CountCommand(CommandBase):
    count: str
    query: Optional[Document] = None


class FindCommand(CommandBase):
    find: str
    filter: Optional[Document] = None
    projection: Optional[Dict[str, OneOrZero]] = None
    allowPartialResults: Optional[bool] = True
    batchSize: Optional[PositiveInt] = 101
    sort: Optional[Dict[str, OneOrMinusOne]] = None
    limit: Optional[NonNegativeInt] = None


class CommandResponse(BaseModel):
    ok: OneOrZero


class CollStatsCommandResponse(CommandResponse):
    ns: str
    size: float
    count: float
    avgObjSize: Optional[float] = None
    storageSize: float
    totalIndexSize: float
    totalSize: float
    scaleFactor: float


class CountCommandResponse(CommandResponse):
    n: NonNegativeInt


class FindCommandResponseCursor(BaseModel):
    firstBatch: List[Document]
    partialResultsReturned: Optional[bool] = None
    id: Optional[int] = None
    ns: str


class FindCommandResponse(CommandResponse):
    cursor: FindCommandResponseCursor


class DeleteCommandDelete(BaseModel):
    q: Document
    limit: OneOrZero
    hint: Optional[Dict[str, OneOrMinusOne]] = None


class DeleteCommand(CommandBase):
    delete: str
    deletes: List[DeleteCommandDelete]


class DeleteCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    writeErrors: Optional[List[Document]] = None


class GetMoreCommand(CommandBase):
    getMore: int
    collection: str
    batchSize: Optional[PositiveInt] = None


class GetMoreCommandResponseCursor(BaseModel):
    nextBatch: List[Document]
    partialResultsReturned: Optional[bool] = None
    id: Optional[int] = None
    ns: str


class GetMoreCommandResponse(CommandResponse):
    cursor: GetMoreCommandResponseCursor


QueryCmd = Union[
    CollStatsCommand, CountCommand, FindCommand, GetMoreCommand, DeleteCommand
]

QueryResponseOptions = Union[
    CollStatsCommandResponse,
    CountCommandResponse,
    FindCommandResponse,
    GetMoreCommandResponse,
    DeleteCommandResponse,
]


def command_response_for(type_):
    d = {
        CollStatsCommand: CollStatsCommandResponse,
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
    result: Optional[Any] = None
    error: Optional[Any] = None

    @model_validator(skip_on_failure=True)
    @classmethod
    def result_xor_error(cls, values):
        result, error = values.get("result"), values.get("error")
        if result is None and error is None:
            raise ValueError("At least one of result and error must be provided.")
        if result is not None and error is not None:
            raise ValueError("Only one of result or error must be provided.")
        return values
