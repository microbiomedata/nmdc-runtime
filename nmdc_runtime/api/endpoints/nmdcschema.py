from importlib.metadata import version

import pymongo
from fastapi import APIRouter, Depends, HTTPException
from nmdc_runtime.util import nmdc_database_collection_names
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


@router.get("/nmdcschema/collection_stats")
def get_nmdc_database_collection_stats(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    Get stats for nmdc:Database MongoDB collections

    Field reference: <https://www.mongodb.com/docs/manual/reference/command/collStats/#std-label-collStats-output>.
    """
    # Take set intersection of
    #   (1) all collections defined by the NMDC schema, and
    #   (2) all runtime collections
    # Thus, only retrieve collections from the schema that are present (i.e. having actual documents) in the runtime.
    present_collection_names = set(nmdc_database_collection_names()) & set(
        mdb.list_collection_names()
    )
    stats = []
    for n in present_collection_names:
        for doc in mdb[n].aggregate(
            [
                {"$collStats": {"storageStats": {}}},
                {
                    "$project": {
                        "ns": 1,
                        "storageStats.size": 1,
                        "storageStats.count": 1,
                        "storageStats.avgObjSize": 1,
                        "storageStats.storageSize": 1,
                        "storageStats.totalIndexSize": 1,
                        "storageStats.totalSize": 1,
                        "storageStats.scaleFactor": 1,
                    }
                },
            ]
        ):
            stats.append(doc)
    return stats


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
    projection: str | None = None,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    for MongoDB-like [projection](https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/): comma-separated list of fields you want the objects in the response to include. Example: `id,doi`
    """
    projection = projection.split(",") if projection else None
    try:
        return strip_oid(
            raise404_if_none(
                mdb[collection_name].find_one({"id": doc_id}, projection=projection)
            )
        )
    except pymongo.errors.OperationFailure as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )
