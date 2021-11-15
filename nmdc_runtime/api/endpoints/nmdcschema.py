from typing import Any

from fastapi import APIRouter, Depends
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.util import ListRequest, ListResponse

router = APIRouter()


@router.get(
    "/nmdcschema/{collection_name}",
    response_model=ListResponse[Any],
    response_model_exclude_unset=True,
)
def list_biosample_set(
    collection_name: str,
    req: ListRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return list_resources(req, mdb, collection_name)


@router.get(
    "/nmdcschema/{collection_name}/{doc_id}",
    response_model=Any,
    response_model_exclude_unset=True,
)
def get_job_info(
    collection_name: str,
    doc_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return raise404_if_none(mdb[collection_name].find_one({"id": doc_id}))
