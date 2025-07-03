import datetime
import json
import logging
from typing import Optional, Any, Dict, List, Union, TypedDict

import bson
import bson.json_util
from pydantic import (
    model_validator,
    Field,
    BaseModel,
    PositiveInt,
    NonNegativeInt,
    field_validator,
    ConfigDict,
    WrapSerializer,
)
from pymongo.database import Database as MongoDatabase
from toolz import assoc, assoc_in
from typing_extensions import Annotated

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, pick
from nmdc_runtime.api.db.mongo import get_mongo_db


def bson_to_json(doc: Any, handler) -> dict:
    """Ensure a dict with e.g. mongo ObjectIds will serialize as JSON."""
    return json.loads(bson.json_util.dumps(doc))


Document = Annotated[Dict[str, Any], WrapSerializer(bson_to_json)]

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
    allowDiskUse: Optional[bool] = False
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


class GetMoreCommand(CommandBase):
    # Note: No `collection` field. See `QueryContinuation` for inter-API-request "sessions" are modeled.
    getMore: str  # Note: runtime uses a `str` id, not an `int` like mongo's native session cursors.
    batchSize: Optional[PositiveInt] = None


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
    # Note: No `ns` field, `id` is a `str`, and `partialResultsReturned` aliased to `queriedShardsUnavailable` to be
    # less confusing to Runtime API clients. See `QueryContinuation` for inter-API-request "sessions" are modeled.
    batch: List[Document]
    partialResultsReturned: Optional[bool] = Field(
        None, alias="queriedShardsUnavailable"
    )
    id: Optional[str] = None

    @field_validator("id", mode="before")
    @classmethod
    def coerce_int_to_str(cls, value: Any) -> Any:
        if isinstance(value, int):
            return str(value)
        else:
            return value


class CursorYieldingCommandResponse(CommandResponse):
    cursor: CommandResponseCursor

    @classmethod
    def slimmed(cls, cmd_response) -> Optional["CursorYieldingCommandResponse"]:
        """Create a new response object that retains only the `_id` for each cursor batch document."""
        dump: dict = cmd_response.model_dump(exclude_unset=True)

        # If any dictionary in this batch lacks an `_id` key, log a warning and return `None`.`
        id_list = [pick(["_id"], batch_doc) for batch_doc in dump["cursor"]["batch"]]
        if any("_id" not in doc for doc in id_list):
            logging.warning("Some documents in the batch lack an `_id` field.")
            return None

        dump = assoc_in(
            dump,
            ["cursor", "batch"],
            id_list,
        )
        return cls(**dump)


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


# Custom types for the `delete_specs` derived from `DeleteStatement`s.
DeleteSpec = TypedDict("DeleteSpec", {"filter": Document, "limit": OneOrZero})
DeleteSpecs = List[DeleteSpec]

# If `multi==True` all documents that meet the query criteria will be updated.
# Else only a single document that meets the query criteria will be updated.
class UpdateStatement(BaseModel):
    q: Document
    u: Document
    upsert: bool = False
    multi: bool = False
    hint: Optional[Dict[str, OneOrMinusOne]] = None


# Custom types for the `update_specs` derived from `UpdateStatement`s.
UpdateSpec = TypedDict("UpdateSpec", {"filter": Document, "limit": OneOrZero})
UpdateSpecs = List[UpdateSpec]

class UpdateCommand(CommandBase):
    update: str
    updates: List[UpdateStatement]


class DocumentUpserted(BaseModel):
    index: NonNegativeInt
    _id: bson.ObjectId


class UpdateCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    nModified: NonNegativeInt
    upserted: Optional[List[DocumentUpserted]] = None
    writeErrors: Optional[List[Document]] = None


QueryCmd = Union[FindCommand, AggregateCommand]

CursorYieldingCommand = Union[
    QueryCmd,
    GetMoreCommand,
]


Cmd = Union[
    CursorYieldingCommand,
    CollStatsCommand,
    CountCommand,
    DeleteCommand,
    UpdateCommand,
]

CommandResponseOptions = Union[
    CursorYieldingCommandResponse,
    CollStatsCommandResponse,
    CountCommandResponse,
    DeleteCommandResponse,
    UpdateCommandResponse,
]


def command_response_for(type_):
    if issubclass(type_, CursorYieldingCommand):
        return CursorYieldingCommandResponse

    d = {
        CollStatsCommand: CollStatsCommandResponse,
        CountCommand: CountCommandResponse,
        DeleteCommand: DeleteCommandResponse,
        UpdateCommand: UpdateCommandResponse,
    }
    return d.get(type_)


_mdb = get_mongo_db()
