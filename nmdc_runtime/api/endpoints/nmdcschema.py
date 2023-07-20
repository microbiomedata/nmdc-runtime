from importlib.metadata import version

from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import dissoc

from nmdc_runtime.api.core.metadata import map_id_to_collection, get_collection_for_id
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db, nmdc_schema_collection_names
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import ListRequest, ListResponse

router = APIRouter()


def verify_collection_name(
    collection_name: str, mdb: MongoDatabase = Depends(get_mongo_db)
):
    names = nmdc_schema_collection_names(mdb)
    if collection_name not in names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Collection name must be one of {names}",
        )


def strip_oid(doc):
    return dissoc(doc, "_id")


@router.get("/nmdcschema/version")
def get_nmdc_schema_version():
    return version("nmdc_schema")


@router.get(
    "/nmdcschema/{collection_name}",
    response_model=ListResponse[Doc],
    response_model_exclude_unset=True,
    dependencies=[Depends(verify_collection_name)],
)
def list_from_collection(
    collection_name: str,
    req: ListRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    rv = list_resources(req, mdb, collection_name)
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


@router.get(
    "/nmdcschema/ids/{doc_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def get_by_id(
    doc_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    id_dict = map_id_to_collection(mdb)
    collection_name = get_collection_for_id(doc_id, id_dict)
    return strip_oid(
        raise404_if_none(
            collection_name and (mdb[collection_name].find_one({"id": doc_id}))
        )
    )


@router.get(
    "/nmdcschema/{collection_name}/{doc_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
    dependencies=[Depends(verify_collection_name)],
)
def get_from_collection_by_id(
    collection_name: str,
    doc_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return strip_oid(raise404_if_none(mdb[collection_name].find_one({"id": doc_id})))
