import json
from importlib.metadata import version
import re
from typing import List, Dict, Annotated

import pymongo
from fastapi import APIRouter, Depends, HTTPException, Path, Query

from nmdc_runtime.api.endpoints.lib.path_segments import (
    parse_path_segment,
    ParsedPathSegment,
)
from nmdc_runtime.api.models.nmdc_schema import RelatedIDs
from nmdc_runtime.config import DATABASE_CLASS_NAME, IS_RELATED_IDS_ENDPOINT_ENABLED
from nmdc_runtime.minter.config import typecodes
from nmdc_runtime.util import (
    decorate_if,
    nmdc_database_collection_names,
    nmdc_schema_view,
)
from pymongo.database import Database as MongoDatabase
from starlette import status
from linkml_runtime.utils.schemaview import SchemaView
from nmdc_schema.nmdc_data import get_nmdc_schema_definition

from nmdc_runtime.api.core.metadata import map_id_to_collection, get_collection_for_id
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    get_collection_names_from_schema,
)
from nmdc_runtime.api.endpoints.util import (
    list_resources,
    strip_oid,
    comma_separated_values,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import ListRequest, ListResponse

router = APIRouter()


def ensure_collection_name_is_known_to_schema(collection_name: str):
    r"""
    Raises an exception if the specified string is _not_ the name of a collection described by the NMDC Schema.
    """
    names = get_collection_names_from_schema()
    if collection_name not in names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Collection name must be one of {sorted(names)}",
        )


@router.get("/nmdcschema/version")
def get_nmdc_schema_version():
    r"""
    Returns a string indicating which version of the [NMDC Schema](https://microbiomedata.github.io/nmdc-schema/)
    the Runtime is using.

    **Note:** The same information—and more—is also available via the `/version` endpoint.
    """
    return version("nmdc_schema")


@router.get("/nmdcschema/typecodes")
def get_nmdc_schema_typecodes() -> List[Dict[str, str]]:
    r"""
    Returns a list of objects, each of which indicates (a) a schema class, and (b) the typecode
    that the minter would use when generating a new ID for an instance of that schema class.

    Each object has three properties:
    - `id`: a string that consists of "nmdc:" + the class name + "_typecode"
    - `schema_class`: a string that consists of "nmdc:" + the class name
    - `name`: the typecode the minter would use when minting an ID for an instance of that class
    """
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


def _parse_prefilter(
    prefilter: Annotated[
        str,
        Path(
            description="Example: `ids=nmdc:bsm-11-6zd5nb38,nmdc:bsm-11-ahpvvb55`.",
            examples=["ids=nmdc:bsm-11-6zd5nb38,nmdc:bsm-11-ahpvvb55"],
        ),
    ],
) -> ParsedPathSegment:
    return parse_path_segment("prefilter;" + prefilter)


def _parse_postfilter(
    postfilter: Annotated[
        str,
        Path(
            description=(
                "Example: `types=nmdc:DataObject,nmdc:PlannedProcess`."
                + '\n\nUse `types=nmdc:NamedThing` to return "all" types'
            ),
            examples=[
                "types=nmdc:NamedThing",
                "types=nmdc:DataObject,nmdc:PlannedProcess",
            ],
        ),
    ],
) -> ParsedPathSegment:
    return parse_path_segment("postfilter;" + postfilter)


@decorate_if(condition=IS_RELATED_IDS_ENDPOINT_ENABLED)(
    router.get(
        "/nmdcschema/related_ids/{prefilter}/{postfilter}",
        response_model=ListResponse[RelatedIDs],
        response_model_exclude_unset=True,
    )
)
def get_related_ids(
    prefilter_parsed: ParsedPathSegment = Depends(_parse_prefilter),
    postfilter_parsed: ParsedPathSegment = Depends(_parse_postfilter),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """From entity IDs captured by `prefilter`, retrieve `postfilter`ed metadata for all related entities.

    Use `postfilter` to control:
    - what related entities are returned (no default: use `types=nmdc:NamedObject` to return "all" types), and
    - what metadata are returned for them (default: `id` and `type`).

    Prefilters:
    - `ids` : one or more (comma-delimited) IDs.
        Example: `ids=nmdc:bsm-11-6zd5nb38,nmdc:bsm-11-ahpvvb55`.
    - `from` : `nmdc:Database` collection from which to retrieve related entities.
        Example: `from=biosample_set`.
    - `match` : MongoDB `filter` to apply to `from` collection. Must be proceeded by `from`.
        Example: `from=study_set;match={"name": {"$regex": "National Ecological Observatory Network"}}`.

    Postfilters:
    - `types=nmdc:DataObject`

    This endpoint performs bidirectional graph traversal from the entity IDs yielded by `prefilter`:

    1. "Inbound" traversal: Follows inbound relationships to find entities that influenced
       each given entity, returning them in the "was_influenced_by" field.

    2. "Outbound" traversal: Follows outbound relationships to find entities that were influenced
       by each given entity, returning them in the "influenced" field.
    """
    prefilter = {}
    for param, value in prefilter_parsed.segment_parameters.items():
        if param == "ids":
            prefilter["ids"] = value if isinstance(value, list) else [value]
        if param == "from":
            prefilter["from"] = value
        if param == "match":
            prefilter["match"] = value

    if "ids" in prefilter:
        if ("from" in prefilter) or ("match" in prefilter):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot use `ids` prefilter parameter with {`from`,`match`} prefilter parameters.",
            )
        ids_found = [
            d["id"]
            for d in mdb.alldocs.find({"id": {"$in": prefilter["ids"]}}, {"id": 1})
        ]
        ids_not_found = list(set(prefilter["ids"]) - set(ids_found))
        if ids_not_found:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Some IDs not found: {ids_not_found}.",
            )
    if "from" in prefilter:
        if prefilter["from"] not in get_collection_names_from_schema():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"`from` parameter {prefilter['from']} is not a known schema collection name."
                    f" Known names: {get_collection_names_from_schema()}."
                ),
            )
    if "match" in prefilter:
        if "from" not in prefilter:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Cannot use `match` prefilter parameter without `from` prefilter parameter.",
            )
        try:
            prefilter["match"] = json.loads(prefilter["match"])
            assert mdb[prefilter["from"]].count_documents(prefilter["match"]) > 0
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
            )

    types = ["nmdc:NamedThing"]
    for param, value in postfilter_parsed.segment_parameters.items():
        if param == "types":
            types = value if isinstance(value, list) else [value]
    types_possible = set([f"nmdc:{name}" for name in nmdc_schema_view().all_classes()])
    types_not_found = list(set(types) - types_possible)
    if types_not_found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Some types not found: {types_not_found}. "
                f"You may need to prefix with `nmdc:`. Types possible: {types_possible}"
            ),
        )

    # Prepare `ids` for the `was_influenced_by` and `influenced` aggregation pipelines.
    if "from" in prefilter:
        ids = [
            d["id"]
            for d in mdb[prefilter["from"]].find(
                filter=prefilter.get("match", {}), projection={"id": 1}
            )
        ]
    else:
        ids = prefilter["ids"]

    # This aggregation pipeline traverses the graph of documents in the alldocs collection, following inbound
    # relationships (_inbound.id) to discover upstream documents that influenced the documents identified by `ids`.
    # It unwinds the collected (via `$graphLookup`) influencers, filters them by given `types` of interest,
    # projects only essential fields to reduce response latency and size, and groups them by each of the given `ids`,
    # i.e. re-winding the `$unwind`-ed influencers into an array for each given ID.
    was_influenced_by = list(
        mdb.alldocs.aggregate(
            [
                {"$match": {"id": {"$in": ids}}},
                {
                    "$graphLookup": {
                        "from": "alldocs",
                        "startWith": "$_inbound.id",
                        "connectFromField": "_inbound.id",
                        "connectToField": "id",
                        "as": "was_influenced_by",
                    }
                },
                {"$unwind": {"path": "$was_influenced_by"}},
                {"$match": {"was_influenced_by._type_and_ancestors": {"$in": types}}},
                {"$project": {"id": 1, "was_influenced_by": "$was_influenced_by"}},
                {
                    "$group": {
                        "_id": "$id",
                        "was_influenced_by": {
                            "$addToSet": {
                                "id": "$was_influenced_by.id",
                                "type": "$was_influenced_by.type",
                            }
                        },
                    }
                },
                {
                    "$lookup": {
                        "from": "alldocs",
                        "localField": "_id",
                        "foreignField": "id",
                        "as": "selves",
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "id": "$_id",
                        "was_influenced_by": 1,
                        "type": {"$arrayElemAt": ["$selves.type", 0]},
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    # This aggregation pipeline traverses the graph of documents in the alldocs collection, following outbound
    # relationships (_outbound.id) to discover downstream documents that were influenced by the documents identified
    # by `ids`. It unwinds the collected (via `$graphLookup`) "influencees", filters them by given `types` of
    # interest, projects only essential fields to reduce response latency and size, and groups them by each of the
    # given `ids`, i.e. re-winding the `$unwind`-ed influencees into an array for each given ID.
    influenced = list(
        mdb.alldocs.aggregate(
            [
                {"$match": {"id": {"$in": ids}}},
                {
                    "$graphLookup": {
                        "from": "alldocs",
                        "startWith": "$_outbound.id",
                        "connectFromField": "_outbound.id",
                        "connectToField": "id",
                        "as": "influenced",
                    }
                },
                {"$unwind": {"path": "$influenced"}},
                {"$match": {"influenced._type_and_ancestors": {"$in": types}}},
                {
                    "$group": {
                        "_id": "$id",
                        "influenced": {
                            "$addToSet": {
                                "id": "$influenced.id",
                                "type": "$influenced.type",
                            }
                        },
                    }
                },
                {
                    "$lookup": {
                        "from": "alldocs",
                        "localField": "_id",
                        "foreignField": "id",
                        "as": "selves",
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "id": "$_id",
                        "influenced": 1,
                        "type": {"$arrayElemAt": ["$selves.type", 0]},
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    relations_by_id = {
        id_: {
            "id": id_,
            "was_influenced_by": [],
            "influenced": [],
        }
        for id_ in ids
    }

    # For each subject document that "was influenced by" or "influenced" any documents, create a dictionary
    # containing that subject document's `id`, its `type`, and the list of `id`s of the
    # documents that it "was influenced by" and "influenced".
    for d in was_influenced_by + influenced:
        relations_by_id[d["id"]]["type"] = d["type"]
        relations_by_id[d["id"]]["was_influenced_by"] += d.get("was_influenced_by", [])
        relations_by_id[d["id"]]["influenced"] += d.get("influenced", [])

    return {"resources": list(relations_by_id.values())}


@router.get(
    "/nmdcschema/ids/{doc_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def get_by_id(
    doc_id: Annotated[
        str,
        Path(
            title="Document ID",
            description="The `id` of the document you want to retrieve.\n\n_Example_: `nmdc:bsm-11-abc123`",
            examples=["nmdc:bsm-11-abc123"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    Retrieves the document having the specified `id`, regardless of which schema-described collection it resides in.
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
    doc_id: Annotated[
        str,
        Path(
            title="Document ID",
            description="The `id` of the document.\n\n_Example_: `nmdc:bsm-11-abc123`",
            examples=["nmdc:bsm-11-abc123"],
        ),
    ],
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
    "/nmdcschema/collection_names",
    response_model=List[str],
    status_code=status.HTTP_200_OK,
)
def get_collection_names():
    """
    Return all valid NMDC Schema collection names, i.e. the names of the slots of [the nmdc:Database class](
    https://w3id.org/nmdc/Database/) that describe database collections.
    """
    return sorted(get_collection_names_from_schema())


@router.get(
    "/nmdcschema/{collection_name}",
    response_model=ListResponse[Doc],
    response_model_exclude_unset=True,
)
def list_from_collection(
    collection_name: Annotated[
        str,
        Path(
            title="Collection name",
            description="The name of the collection.\n\n_Example_: `biosample_set`",
            examples=["biosample_set"],
        ),
    ],
    req: Annotated[ListRequest, Query()],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    Retrieves resources that match the specified filter criteria and reside in the specified collection.

    Searches the specified collection for documents matching the specified `filter` criteria.
    If the `projection` parameter is used, each document in the response will only include
    the fields specified by that parameter (plus the `id` field).

    Use the [`GET /nmdcschema/collection_names`](/nmdcschema/collection_names) API endpoint to return all valid
    collection names, i.e. the names of the slots of [the nmdc:Database class](https://w3id.org/nmdc/Database/) that
    describe database collections.

    Note: If the specified maximum page size is a number greater than zero, and _more than that number of resources_
          in the collection match the filter criteria, this endpoint will paginate the resources. Pagination can take
          a long time—especially for collections that contain a lot of documents (e.g. millions).

    **Tips:**
    1. When the filter includes a regex and you're using that regex to match the beginning of a string, try to ensure
       the regex is a [prefix expression](https://www.mongodb.com/docs/manual/reference/operator/query/regex/#index-use),
       That will allow MongoDB to optimize the way it uses the regex, making this API endpoint respond faster.
    """

    # raise HTTP_400_BAD_REQUEST on invalid collection_name
    ensure_collection_name_is_known_to_schema(collection_name)

    rv = list_resources(req, mdb, collection_name)
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


@router.get(
    "/nmdcschema/{collection_name}/{doc_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def get_from_collection_by_id(
    collection_name: Annotated[
        str,
        Path(
            title="Collection name",
            description="The name of the collection.\n\n_Example_: `biosample_set`",
            examples=["biosample_set"],
        ),
    ],
    doc_id: Annotated[
        str,
        Path(
            title="Document ID",
            description="The `id` of the document you want to retrieve.\n\n_Example_: `nmdc:bsm-11-abc123`",
            examples=["nmdc:bsm-11-abc123"],
        ),
    ],
    projection: Annotated[
        str | None,
        Query(
            title="Projection",
            description="""Comma-delimited list of the names of the fields you want the document in the response to
                include.\n\n_Example_: `id,name,ecosystem_type`""",
            examples=[
                "id,name,ecosystem_type",
            ],
        ),
    ] = None,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    Retrieves the document having the specified `id`, from the specified collection; optionally, including only the
    fields specified via the `projection` parameter.
    """
    # raise HTTP_400_BAD_REQUEST on invalid collection_name
    ensure_collection_name_is_known_to_schema(collection_name)

    projection = comma_separated_values(projection) if projection else None
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
