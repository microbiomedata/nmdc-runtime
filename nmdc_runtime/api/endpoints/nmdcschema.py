from importlib.metadata import version
import re
from typing import List, Dict, Annotated

import pymongo
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import AfterValidator
from refscan.lib.helpers import (
    get_collection_names_from_schema,
    get_names_of_classes_eligible_for_collection,
)

from nmdc_runtime.api.endpoints.lib.linked_instances import gather_linked_instances
from nmdc_runtime.config import IS_LINKED_INSTANCES_ENDPOINT_ENABLED
from nmdc_runtime.minter.config import typecodes
from nmdc_runtime.minter.domain.model import check_valid_ids
from nmdc_runtime.util import (
    decorate_if,
    nmdc_database_collection_names,
    nmdc_schema_view,
)
from pymongo.database import Database as MongoDatabase
from starlette import status

from nmdc_runtime.api.core.metadata import map_id_to_collection, get_collection_for_id
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
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
    schema_view = nmdc_schema_view()
    names = get_collection_names_from_schema(schema_view)
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


@decorate_if(condition=IS_LINKED_INSTANCES_ENDPOINT_ENABLED)(
    router.get(
        "/nmdcschema/linked_instances",
        response_model=ListResponse[Doc],
        response_model_exclude_unset=True,
    )
)
def get_linked_instances(
    ids: Annotated[
        list[str],
        Query(
            title="Instance (aka Document) IDs",
            description=(
                "The `ids` you want to serve as the nexus for graph traversal to collect linked instances."
                "\n\n_Example_: [`nmdc:dobj-11-nf3t6f36`]"
            ),
            examples=["nmdc:dobj-11-nf3t6f36"],
        ),
        AfterValidator(check_valid_ids),
    ],
    types: Annotated[
        list[str] | None,
        Query(
            title="Instance (aka Document) types",
            description=(
                "The `types` of instances you want to return. Can be abstract types such as `nmdc:InformationObject` "
                "or instantiated ones such as `nmdc:DataObject`. Defaults to [`nmdc:NamedThing`]."
                "\n\n_Example_: [`nmdc:PlannedProcess`]"
            ),
            examples=["nmdc:bsm-11-abc123"],
        ),
    ] = None,
    page_token: Annotated[
        str | None,
        Query(
            title="Next page token",
            description="""A bookmark you can use to fetch the _next_ page of resources. You can get this from the
                    `next_page_token` field in a previous response from this endpoint.\n\n_Example_: 
                    `nmdc:sys0zr0fbt71`""",
            examples=[
                "nmdc:sys0zr0fbt71",
            ],
        ),
    ] = None,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    Retrieves database instances that are both (a) linked to any of `ids`, and (b) of a type in `types`.

    An [instance](https://linkml.io/linkml-model/latest/docs/specification/02instances/) is an object conforming to
    a class definition ([linkml:ClassDefinition](https://w3id.org/linkml/ClassDefinition))
    in our database ([nmdc:Database](https://w3id.org/nmdc/Database)).
    While a [nmdc:Database](https://w3id.org/nmdc/Database) is organized into collections,
    every item in every database collection -- that is, every instance -- knows its `type`, so we can
    (and here do)<sup>&dagger;</sup>
    return a simple list of instances
    ([a LinkML CollectionInstance](https://linkml.io/linkml-model/latest/docs/specification/02instances/#collections)),
    which a client may use to construct a corresponding [nmdc:Database](https://w3id.org/nmdc/Database).

    From the nexus instance IDs given in `ids`, both "upstream" and "downstream" links are followed (transitively) in
    order to collect the set of all instances linked to these `ids`.

    * A link "upstream" is represented by a slot ([linkml:SlotDefinition](https://w3id.org/linkml/SlotDefinition))
    for which the
    range ([linkml:range](https://w3id.org/linkml/range)) instance has originated, or helped produce,
    the domain ([linkml:domain](https://w3id.org/linkml/domain)) instance.
    For example, we consider [nmdc:associated_studies](https://w3id.org/nmdc/associated_studies) to be
    an "upstream" slot because we consider a [nmdc:Study](https://w3id.org/nmdc/Study) (the slot's range)
    to be upstream of a [nmdc:Biosample](https://w3id.org/nmdc/Biosample) (the slot's domain).

    * A link "downstream" is represented by a slot for which the
    range instance has originated from, or was in part produced by, the domain instance.
    For example, [nmdc:has_output](https://w3id.org/nmdc/has_output) is
    a "downstream" slot because its [nmdc:NamedThing](https://w3id.org/nmdc/NamedThing) range
    is downstream of its [nmdc:PlannedProcess](https://w3id.org/nmdc/PlannedProcess) domain.

    Acceptable values for `types` are not limited only to the ones embedded in concrete instances, e.g.
    the `schema_class` field values returned by the [`GET /nmdcschema/typecodes`](/nmdcschema/typecodes) API endpoint.
    Rather, any subclass (of any depth) of [nmdc:NamedThing](https://w3id.org/nmdc/NamedThing) --
    [nmdc:DataEmitterProcess](https://w3id.org/nmdc/DataEmitterProcess),
    [nmdc:InformationObject](https://w3id.org/nmdc/InformationObject),
    [nmdc:Sample](https://w3id.org/nmdc/Sample), etc. -- may be given.
    If no value for `types` is given, then all [nmdc:NamedThing](https://w3id.org/nmdc/NamedThing)s are returned.

    <sup>&dagger;</sup>: actually, we do not (yet).
    For now (see [microbiomedata/nmdc-runtime#1118](https://github.com/microbiomedata/nmdc-runtime/issues/1118)),
    we return a short list of "fat" documents,  each of which represents one of the `ids` and presents
    representations of that id's downstream and upstream instances (currently just each instance's `id` and `type`)
    as separate subdocument array fields.
    """
    # TODO move logic from endpoint to unit-testable handler
    # TODO ListResponse[SimplifiedNMDCDatabase]
    # TODO ensure pagination for responses
    ids_found = [d["id"] for d in mdb.alldocs.find({"id": {"$in": ids}}, {"id": 1})]
    ids_not_found = list(set(ids) - set(ids_found))
    if ids_not_found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Some IDs not found: {ids_not_found}.",
        )
    types = types or ["nmdc:NamedThing"]
    types_possible = set([f"nmdc:{name}" for name in nmdc_schema_view().all_classes()])
    types_not_found = list(set(types) - types_possible)
    if types_not_found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Some types not found: {types_not_found}. "
                "You may need to prefix with `nmdc:`. "
                "If you don't supply any types, the set {'nmdc:NamedThing'} will be used. "
                f"Types possible: {types_possible}"
            ),
        )

    # TODO ~~actually, need `merge_into_collection_name = fn(ids,types)`~~
    #   ~~in order to accommodate `page_token` without overhaul of `list_resources` logic.~~
    #   Wait, `mdb.page_tokens` docs are `{"_id": req.page_token, "ns": collection_name}`,
    #   i.e. `page_token` is globally unique, so can just look up `collection_name` via doc `ns` field.

    merge_into_collection_name = gather_linked_instances(
        alldocs_collection=mdb.alldocs, ids=ids, types=types
    )

    rv = list_resources(
        ListRequest(page_token=page_token), mdb, merge_into_collection_name
    )
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


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
    schema_view = nmdc_schema_view()
    collection_names = []
    for collection_name in get_collection_names_from_schema(schema_view=schema_view):
        if schema_class_name in get_names_of_classes_eligible_for_collection(
            schema_view=schema_view, collection_name=collection_name
        ):
            collection_names.append(collection_name)

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
    schema_view = nmdc_schema_view()
    return sorted(get_collection_names_from_schema(schema_view))


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
