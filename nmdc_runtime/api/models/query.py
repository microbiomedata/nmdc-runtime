import datetime
from typing import Optional, Any, Dict, List, Union

from bson import ObjectId
from pydantic import (
    model_validator,
    Field,
    BaseModel,
    PositiveInt,
    NonNegativeInt,
    field_validator,
    ConfigDict,
)
from toolz import assoc
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


class AggregateCommand(CommandBase):
    aggregate: str
    pipeline: List[Document]
    cursor: Optional[Document] = None

    @field_validator("pipeline")
    @classmethod
    def disallow_invalid_pipeline_stages(
        cls, pipeline: List[Document]
    ) -> List[Document]:
        deny_list = ["$out", "$merge"]

        if any(
            key in deny_list for pipeline_stage in pipeline for key in pipeline_stage
        ):
            raise ValueError("$Out and $merge pipeline stages are not allowed.")

        return pipeline

    @model_validator(mode="before")
    @classmethod
    def ensure_default_value_for_cursor(cls, data: Any) -> Document:
        if isinstance(data, dict) and "cursor" not in data:
            return assoc(data, "cursor", {"batchSize": 25})
        return data


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


class CommandResponseCursor(BaseModel):
    firstBatch: List[Document]
    partialResultsReturned: Optional[bool] = None
    id: Optional[int] = None
    ns: str


class FindCommandResponse(CommandResponse):
    cursor: CommandResponseCursor


class AggregateCommandResponse(CommandResponse):
    # model_config = ConfigDict(extra="allow")
    cursor: CommandResponseCursor


class DeleteStatement(BaseModel):
    q: Document
    limit: OneOrZero
    hint: Optional[Dict[str, OneOrMinusOne]] = None


class DeleteCommand(CommandBase):
    delete: str
    deletes: List[DeleteStatement]


class DeleteCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    writeErrors: Optional[List[Document]] = None


# If `multi==True` all documents that meet the query criteria will be updated.
# Else only a single document that meets the query criteria will be updated.
class UpdateStatement(BaseModel):
    q: Document
    u: Document
    upsert: bool = False
    multi: bool = False
    hint: Optional[Dict[str, OneOrMinusOne]] = None


class UpdateCommand(CommandBase):
    update: str
    updates: List[UpdateStatement]


class DocumentUpserted(BaseModel):
    index: NonNegativeInt
    _id: ObjectId


class UpdateCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    nModified: NonNegativeInt
    upserted: Optional[List[DocumentUpserted]] = None
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
    CollStatsCommand,
    CountCommand,
    FindCommand,
    GetMoreCommand,
    DeleteCommand,
    UpdateCommand,
    AggregateCommand,
]

QueryResponseOptions = Union[
    CollStatsCommandResponse,
    CountCommandResponse,
    FindCommandResponse,
    GetMoreCommandResponse,
    DeleteCommandResponse,
    UpdateCommandResponse,
    AggregateCommandResponse,
]


def command_response_for(type_):
    d = {
        CollStatsCommand: CollStatsCommandResponse,
        CountCommand: CountCommandResponse,
        FindCommand: FindCommandResponse,
        GetMoreCommand: GetMoreCommandResponse,
        DeleteCommand: DeleteCommandResponse,
        UpdateCommand: UpdateCommandResponse,
        AggregateCommand: AggregateCommandResponse,
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

    @model_validator(mode="before")
    def result_xor_error(cls, values):
        result, error = values.get("result"), values.get("error")
        if result is None and error is None:
            raise ValueError("At least one of result and error must be provided.")
        if result is not None and error is not None:
            raise ValueError("Only one of result or error must be provided.")
        return values
