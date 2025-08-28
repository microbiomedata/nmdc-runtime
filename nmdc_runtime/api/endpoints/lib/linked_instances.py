"""

This module houses logic for the `GET /nmdcschema/linked_instances` endpoint, defined as
`nmdc_runtime.api.endpoints.nmdcschema.linked_instances`, to avoid (further) bloating the
`nmdc_runtime.api.endpoints.nmdcschema` module.

"""

from typing import Literal, Any

from bson import ObjectId
from pymongo.collection import Collection as MongoCollection

from nmdc_runtime.api.core.util import hash_from_str


def hash_from_ids_and_types(ids: list[str], types: list[str]) -> str:
    """A quick hash as a function of `ids` and `types`.

    This will serve as part of a temporary mongo collection name.
    Because it will only be "part of" the name, avoiding hash collisions isn't a priority.

    Returns a hex digest truncated to 8 characters, so 16**8 ≈ 4M possible values.
    """
    return hash_from_str(
        ",".join(sorted(ids)) + "." + ",".join(sorted(types)), algo="md5"
    )[:8]


def temp_linked_instances_collection_name(ids: list[str], types: list[str]) -> str:
    """A name for a temporary mongo collection to store linked instances in service of an API request."""
    return f"_runtime.tmp.linked_instances.{hash_from_ids_and_types(ids=ids,types=types)}.{ObjectId()}"


def gather_linked_instances(
    alldocs_collection: MongoCollection,
    ids: list[str],
    types: list[str],
) -> str:
    """Collect linked instances and stores them in a new temporary collection.

    Run an aggregation pipeline over `alldocs_collection` that collects ∈`types` instances linked to `ids`.
    The pipeline is run twice, once for each of {"downstream", "upstream"} directions.
    """
    merge_into_collection_name = temp_linked_instances_collection_name(
        ids=ids, types=types
    )
    for direction in ["downstream", "upstream"]:
        _ = list(
            alldocs_collection.aggregate(
                pipeline_for_direction(
                    ids=ids,
                    types=types,
                    direction=direction,
                    merge_into_collection_name=merge_into_collection_name,
                ),
                allowDiskUse=True,
            )
        )
    return merge_into_collection_name


def pipeline_for_direction(
    ids: list[str],
    types: list[str],
    direction: Literal["downstream", "upstream"],
    merge_into_collection_name: str,
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
            merge_into_collection_name=merge_into_collection_name, direction=direction
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
    merge_into_collection_name: str,
    direction: Literal["downstream", "upstream"],
) -> dict[str, Any]:
    """
    Returns an aggregation-pipeline step that merges its input document stream to a collection dedicated to serving
    the caller in a manner amenable to pagination across multiple HTTP requests.
    """
    return {
        "$merge": {
            "into": merge_into_collection_name,
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
