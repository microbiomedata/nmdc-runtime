"""

This module houses logic for the `GET /nmdcschema/linked_instances` endpoint, defined as
`nmdc_runtime.api.endpoints.nmdcschema.linked_instances`, to avoid (further) bloating the
`nmdc_runtime.api.endpoints.nmdcschema` module.

"""

import logging
from datetime import timedelta
from typing import Literal, Any

from bson import ObjectId
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase
from toolz import merge

from nmdc_runtime.api.core.util import hash_from_str, now
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.util import (
    duration_logger,
    get_class_name_to_collection_names_map,
    nmdc_schema_view,
)


def hash_from_ids_and_types(ids: list[str], types: list[str]) -> str:
    """A quick hash as a function of `ids` and `types`.

    This will serve as part of a temporary mongo collection name.
    Because it will only be "part of" the name, avoiding hash collisions isn't a priority.

    Returns a hex digest truncated to 8 characters, so 16**8 ≈ 4M possible values.
    """
    return hash_from_str(
        ",".join(sorted(ids)) + "." + ",".join(sorted(types)), algo="md5"
    )[:8]


def generate_temp_linked_instances_collection_name(ids: list[str], types: list[str]) -> str:
    """A name for a temporary mongo collection to store linked instances in service of an API request."""
    return f"_runtime.tmp.linked_instances.{hash_from_ids_and_types(ids=ids,types=types)}.{ObjectId()}"


def drop_stale_temp_linked_instances_collections() -> None:
    """Drop any temporary linked-instances collections that were generated earlier than one day ago."""
    mdb = get_mongo_db()
    one_day_ago = now() - timedelta(days=1)
    for collection_name in mdb.list_collection_names(
        filter={"name": {"$regex": r"^_runtime.tmp.linked_instances\..*"}}
    ):
        if ObjectId(collection_name.split(".")[-1]).generation_time < one_day_ago:
            mdb.drop_collection(collection_name)


def gather_linked_instances(
    alldocs_collection: MongoCollection,
    ids: list[str],
    types: list[str],
) -> str:
    """Collect linked instances and stores them in a new temporary collection.

    Run an aggregation pipeline over `alldocs_collection` that collects ∈`types` instances linked to `ids`.
    The pipeline is run twice, once for each of {"downstream", "upstream"} directions.
    """
    temp_linked_instances_collection_name = generate_temp_linked_instances_collection_name(
        ids=ids, types=types
    )
    for direction in ["downstream", "upstream"]:
        with duration_logger(logging.info, f"Gathering {direction} linked instances"):
            _ = list(
                alldocs_collection.aggregate(
                    pipeline_for_direction(
                        ids=ids,
                        types=types,
                        direction=direction,
                        name_of_collection_to_merge_into=temp_linked_instances_collection_name,
                    ),
                    allowDiskUse=True,
                )
            )
    return temp_linked_instances_collection_name


def pipeline_for_direction(
    ids: list[str],
    types: list[str],
    direction: Literal["downstream", "upstream"],
    name_of_collection_to_merge_into: str,
    alldocs_collection_name: str = "alldocs",
) -> list:
    """A pure function that returns the aggregation pipeline for `direction`.

    The pipeline
    - collects ∈`types` instances linked to `ids` along `direction`,
    - retains only those document fields essential to the caller, and
    - ensures the collected instances are present, and properly updated if applicable, in a merge-target collection.
    """
    return pipeline_for_instances_linked_to_ids_by_direction(
        ids=ids,
        types=types,
        direction=direction,
        alldocs_collection_name=alldocs_collection_name,
    ) + [
        {"$project": {"id": 1, "type": 1, f"_{direction}_of": 1}},
        pipeline_stage_for_merging_instances_and_grouping_link_provenance_by_direction(
            name_of_collection_to_merge_into=name_of_collection_to_merge_into,
            direction=direction,
        ),
    ]


def pipeline_for_instances_linked_to_ids_by_direction(
    ids: list[str],
    types: list[str],
    direction: Literal["downstream", "upstream"],
    alldocs_collection_name: str = "alldocs",
    slim: bool = True,
) -> list[dict[str, Any]]:
    """
    Returns an aggregation pipeline that:
    - traverses the graph of documents in the alldocs collection, following `direction`-specific relationships
      to discover documents linked to the documents given by `ids`.
    - `$unwind`s the collected (via `$graphLookup`) docs,
    - filters them by given `types` of interest,
     - adds bookkeeping information about `direction`ality, and
     - (optionally) projects only essential fields to reduce response latency and size.
    """
    return [
        {"$match": {"id": {"$in": ids}}},
        {
            "$graphLookup": {
                "from": alldocs_collection_name,
                "startWith": f"$_{direction}.id",
                "connectFromField": f"_{direction}.id",
                "connectToField": "id",
                "as": f"{direction}_docs",
            }
        },
        {"$unwind": {"path": f"${direction}_docs"}},
        {"$match": {f"{direction}_docs._type_and_ancestors": {"$in": types}}},
        {"$addFields": {f"{direction}_docs._{direction}_of": ["$id"]}},
        {"$replaceRoot": {"newRoot": f"${direction}_docs"}},
    ] + ([{"$project": {"id": 1, "type": 1, f"_{direction}_of": 1}}] if slim else [])


def pipeline_stage_for_merging_instances_and_grouping_link_provenance_by_direction(
    name_of_collection_to_merge_into: str,
    direction: Literal["downstream", "upstream"],
) -> dict[str, Any]:
    """
    Returns an aggregation-pipeline step that merges its input document stream to a collection dedicated to serving
    the caller in a manner amenable to pagination across multiple HTTP requests.
    """
    return {
        "$merge": {
            "into": name_of_collection_to_merge_into,
            "on": "_id",
            "whenMatched": [
                {
                    "$set": {
                        f"_{direction}_of": {
                            "$setUnion": [
                                f"$_{direction}_of",
                                f"$$new._{direction}_of",
                            ]
                        }
                    }
                }
            ],
            "whenNotMatched": "insert",
        }
    }


def hydrated(resources: list[dict], mdb: MongoDatabase) -> list[dict]:
    """Replace each `dict` in `resources` with a hydrated version.

    Instead of returning the retrieved "full" documents as is, we merge each one with (a copy of) the corresponding
    original document in *resources*, which includes additional fields, e.g. `_upstream_of` and `_downstream_of`.
    """
    class_name_to_collection_names_map = get_class_name_to_collection_names_map(
        nmdc_schema_view()
    )
    types_of_resources = {r["type"] for r in resources}
    full_docs_by_id = {}

    for type in types_of_resources:
        resource_ids_of_type = [d["id"] for d in resources if d["type"] == type]
        schema_collection = mdb.get_collection(
            # Note: We are assuming that documents of a given type are only allowed (by the schema) to reside in one
            # collection. Based on that assumption, we will query only the _first_ collection whose name we get from
            # the map. This assumption is continuously verified prior to code deployment via
            # `test_get_class_name_to_collection_names_map_has_one_and_only_one_collection_name_per_class_name`.
            class_name_to_collection_names_map[type.removeprefix("nmdc:")][0]
        )
        for doc in schema_collection.find({"id": {"$in": resource_ids_of_type}}):
            full_docs_by_id[doc["id"]] = doc

    return [merge(r, full_docs_by_id[r["id"]]) for r in resources]
