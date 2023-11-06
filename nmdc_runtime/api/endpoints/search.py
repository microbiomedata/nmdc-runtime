import json

from fastapi import APIRouter, Depends
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.nmdcschema import strip_oid
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.nmdc_schema import (
    DataObject,
    DataObjectListRequest,
    list_request_filter_to_mongo_filter,
)
from nmdc_runtime.api.models.util import ListResponse, ListRequest

router = APIRouter()


@router.get(
    "/data_objects",
    response_model=ListResponse[DataObject],
    response_model_exclude_unset=True,
)
def data_objects(
    req: DataObjectListRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    filter_ = list_request_filter_to_mongo_filter(req.model_dump(exclude_unset=True))
    max_page_size = filter_.pop("max_page_size", None)
    page_token = filter_.pop("page_token", None)
    req = ListRequest(
        filter=json.dumps(filter_),
        max_page_size=max_page_size,
        page_token=page_token,
    )
    rv = list_resources(req, mdb, "data_objects")
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv
