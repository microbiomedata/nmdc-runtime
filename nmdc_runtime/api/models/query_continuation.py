"""
A *query continuation* is a means to effectively resume a query, i.e. a `find` or `aggregate` MongoDB database command.

A *query continuation* document represents a *continuation* (cf. <https://en.wikipedia.org/wiki/Continuation>) for a
query and uses a stored value ("cursor") for MongoDB's guaranteed unique-valued document field, `_id`,
such that the documents returned by the command are guaranteed to be sorted in ascending order by `_id`.

In this way, an API client may retrieve all documents defined by a `find` or `aggregate` command over multiple HTTP
requests. One can think of this process as akin to pagination; however, with "cursor-based" pagination, there are no
guarantees wrt a fixed "page size".

"""

import datetime
import logging
import json

from pydantic import BaseModel, Field
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.query import (
    CommandResponse,
    QueryCmd,
)

COLLECTION_NAME_FOR_QUERY_CONTINUATIONS = "_runtime.query_continuations"

_mdb: MongoDatabase = get_mongo_db()
_qc_collection = _mdb[COLLECTION_NAME_FOR_QUERY_CONTINUATIONS]

# Ensure one-hour TTL on `_runtime.query_continuations` documents via TTL Index.
# Reference: https://www.mongodb.com/docs/manual/core/index-ttl/
_qc_collection.create_index(
    {"last_modified": 1}, expireAfterSeconds=3600
)


def not_empty(lst: list) -> bool:
    return len(lst) > 0


class QueryContinuation(BaseModel):
    """A query that has not completed, and that may be resumed, using `cursor` to modify `query_cmd`.

    This model is intended to represent the state of a logical "session" to "page" through a query's results
    over several HTTP requests, and may be discarded after fetching all "batches" of documents.

    Thus, a mongo collection tracking query continuations may be reasonably given e.g. a so-called "TTL Index"
    for the `last_modified` field, assuming that `last_modified` is updated each time `query` is updated.
    """

    id: str = Field(..., alias="_id")
    query_cmd: QueryCmd
    cursor: str
    last_modified: datetime.datetime


class QueryContinuationError(Exception):
    def __init__(self, detail: str):
        self.detail = detail

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.detail})"


def dump_qc(m: BaseModel):
    return m.model_dump(by_alias=True, exclude_unset=True)


def create_qc(query_cmd: QueryCmd, cmd_response: CommandResponse) -> QueryContinuation:
    """Creates query continuation from command and response, and persists continuation to database."""

    logging.info(f"cmd_response: {cmd_response}")
    last_id = json.dumps(cmd_response.cursor.batch[-1]["_id"])
    logging.info(f"Last document ID for query continuation: {last_id}")
    cc = QueryContinuation(
        _id=generate_one_id(_mdb, "query_continuation"),
        query_cmd=query_cmd,
        cursor=last_id,
        last_modified=now(),
    )
    _qc_collection.insert_one(dump_qc(cc))
    return cc


def get_qc_by__id(_id: str) -> QueryContinuation | None:
    r"""
    Returns the `QueryContinuation` having the specified `_id` value, raising an exception
    if the corresponding document does not exist in the database.
    """
    doc = _qc_collection.find_one({"_id": _id})
    if doc is None:
        raise QueryContinuationError(f"cannot find cc with id {_id}")
    return QueryContinuation(**doc)


def get_last_doc__id_for_qc(query_continuation: QueryContinuation) -> str:
    """
    Retrieve the last document `_id` for the given `QueryContinuation`.
    """
    # Assuming `query_continuation` has an attribute `cursor` that stores the last document _id
    logging.info(
        f"Cursor for last doc query continuation: {query_continuation.cursor}"
    )
    return json.loads(query_continuation.cursor)


def get_initial_query_for_qc(query_continuation: QueryContinuation) -> QueryCmd:
    """
    Retrieve the initial query command for the given `QueryContinuation`.
    """
    return query_continuation.query_cmd
