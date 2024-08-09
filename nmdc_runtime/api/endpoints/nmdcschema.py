from importlib.metadata import version
import re

import pymongo
from fastapi import APIRouter, Depends, HTTPException

from nmdc_runtime.config import DATABASE_CLASS_NAME
from nmdc_runtime.minter.config import typecodes
from nmdc_runtime.util import nmdc_database_collection_names
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import dissoc
from linkml_runtime.utils.schemaview import SchemaView
from nmdc_schema.nmdc_data import get_nmdc_schema_definition

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
    """
    To view the [NMDC Schema](https://microbiomedata.github.io/nmdc-schema/) version the database is currently using,
    try executing the GET /nmdcschema/version endpoint
    """
    return version("nmdc_schema")


@router.get("/nmdcschema/typecodes")
def get_nmdc_schema_typecodes():
    return typecodes()


@router.get("/nmdcschema/collection_stats")
def get_nmdc_database_collection_stats(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    To get the NMDC Database MongoDB collection statistics, like the total count of records in a collection or the size
    of the collection, try executing the GET /nmdcschema/collection_stats endpoint

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
    """
    The GET /nmdcschema/{collection_name} endpoint is a general purpose way to retrieve metadata about a specified
    collection given user-provided filter and projection criteria. Please see the [Collection Names](https://microbiomedata.github.io/nmdc-schema/Database/)
    that may be retrieved. Please note that metadata may only be retrieved about one collection at a time.
    """
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
    """
    If the identifier of the record is known, the GET /nmdcshema/ids/{doc_id} can be used to retrieve the specified record.
    \n Note that only one identifier may be used at a time, and therefore, only one record may be retrieved at a time using this method.
    """
    id_dict = map_id_to_collection(mdb)
    collection_name = get_collection_for_id(doc_id, id_dict)
    return strip_oid(
        raise404_if_none(
            collection_name and (mdb[collection_name].find_one({"id": doc_id}))
        )
    )


@router.get("/nmdcschema/ids/{doc_id}/collection-name")
def get_collection_name_by_doc_id(
    doc_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    Returns the name of the collection, if any, containing the document having the specified `id`.

    This endpoint uses the NMDC Schema to determine the schema class of which an instance could have
    the specified value as its `id`; and then uses the NMDC Schema to determine the names of the
    `Database` slots (i.e. Mongo collection names) that could contain instances of that schema class.

    This endpoint then searches those Mongo collections for a document having that `id`.
    If it finds one, it responds with the name of the collection containing the document.
    If it does not find one, it response with an `HTTP 404 Not Found` response.
    """
    # Note: The `nmdc_runtime.api.core.metadata.map_id_to_collection` function is
    #       not used here because that function (a) only processes collections whose
    #       names end with `_set` and (b) only works for `id` values that are in
    #       use in the database (as opposed to hypothetical `id` values).

    # Extract the typecode portion, if any, of the specified `id`.
    #
    # Examples:
    # - "nmdc:foo-123-456" → "foo"
    # - "foo:nmdc-123-456" → `None`
    #
    pattern = re.compile(r"^nmdc:(\w+)?-")
    match = pattern.search(doc_id)
    typecode_portion = match.group(1) if match else None

    if typecode_portion is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The typecode portion of the specified `id` is invalid.",
        )

    # Determine the schema class, if any, of which the specified `id` could belong to an instance.
    schema_class_name = None
    for typecode in typecodes():
        if typecode_portion == typecode["name"]:
            schema_class_name_prefixed = typecode["schema_class"]
            schema_class_name = schema_class_name_prefixed.replace("nmdc:", "", 1)
            break

    if schema_class_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The specified `id` is not compatible with any schema classes.",
        )

    # Determine the Mongo collection(s) in which instances of that schema class can reside.
    collection_names = []
    schema_view = SchemaView(get_nmdc_schema_definition())
    for slot_name in schema_view.class_slots(DATABASE_CLASS_NAME):
        slot_definition = schema_view.induced_slot(slot_name, DATABASE_CLASS_NAME)

        # If this slot doesn't represent a Mongo collection, abort this iteration.
        if not (slot_definition.multivalued and slot_definition.inlined_as_list):
            continue

        # Determine the names of the classes whose instances can be stored in this collection.
        name_of_eligible_class = slot_definition.range
        names_of_eligible_classes = schema_view.class_descendants(
            name_of_eligible_class
        )
        if schema_class_name in names_of_eligible_classes:
            collection_names.append(slot_name)

    if len(collection_names) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The specified `id` is not compatible with any database collections.",
        )

    # Use the Mongo database to determine which of those collections a document having that `id` actually
    # resides in, if any. If multiple collections contain such a document, report only the first one.
    containing_collection_name = None
    for collection_name in collection_names:
        collection = mdb.get_collection(name=collection_name)
        if collection.count_documents(dict(id=doc_id), limit=1) > 0:
            containing_collection_name = collection_name
            break

    if containing_collection_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The specified `id` does not belong to any documents.",
        )

    return {
        "id": doc_id,
        "collection_name": containing_collection_name,
    }


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
    If both the identifier and the collection name of the desired record is known, the
    GET /nmdcschema/{collection_name}/{doc_id} can be used to retrieve the record. The projection parameter is optionally
    available for this endpoint to retrieve only desired attributes from a record. Please note that only one record can
    be retrieved at one time using this method.

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
