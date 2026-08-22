"""Dagster ops related to the MongoDB collection named "alldocs"."""

from collections import defaultdict
from itertools import chain

from bson import ObjectId
from dagster import Failure, OpExecutionContext, op, In, Nothing
from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase
from refscan.lib.helpers import get_names_of_classes_in_effective_range_of_slot
from toolz.dicttoolz import keyfilter

from nmdc_runtime.util import (
    collection_name_to_class_names,
    nmdc_schema_view,
    populated_schema_collection_names_with_id_field,
)


# batch size for writing documents to alldocs
BULK_WRITE_BATCH_SIZE = 2000


def _add_linked_instances_to_alldocs(
    temp_collection: MongoCollection,
    context: OpExecutionContext,
    document_reference_ranged_slots_by_type: dict,
) -> None:
    """
    Adds {`_upstream`,`_downstream`} fields to each document in the temporary alldocs collection.

    The {`_upstream`,`_downstream`} fields each contain an array of subdocuments, each with fields `id` and `type`.
    Each subdocument represents a link to another document that either links to or is linked from the document via
    document-reference-ranged slots. If document A links to document B, document A is not necessarily "upstream of"
    document B. Rather, "upstream" and "downstream" are defined by domain semantics. For example, a Study is
    considered upstream of a Biosample even though the link `associated_studies` goes from a Biosample to a Study.

    Args:
        temp_collection: The temporary MongoDB collection to process
        context: The Dagster execution context for logging
        document_reference_ranged_slots_by_type: Dictionary mapping document types to their reference-ranged slot names

    Returns:
        None (modifies the documents in place)
    """

    context.log.info(
        "Building relationships and adding `_upstream` and `_downstream` fields..."
    )

    # document ID -> type (with "nmdc:" prefix preserved)
    id_to_type_map: dict[str, str] = {}

    # set of (<referencing document ID>, <slot>, <referenced document ID>) 3-tuples.
    relationship_triples: set[tuple[str, str, str]] = set()

    # Collect relationship triples.
    for doc in temp_collection.find():
        doc_id = doc["id"]
        # Store the full type with prefix intact
        doc_type = doc["type"]

        # Record ID to type mapping - preserve the original type with prefix
        id_to_type_map[doc_id] = doc_type

        # Find all document references from this document
        reference_slots = document_reference_ranged_slots_by_type.get(doc_type, [])
        for slot in reference_slots:
            if slot in doc:
                # Handle both single-value and array references
                refs = doc[slot] if isinstance(doc[slot], list) else [doc[slot]]
                for ref_doc in temp_collection.find(
                    {"id": {"$in": refs}}, ["id", "type"]
                ):
                    id_to_type_map[ref_doc["id"]] = ref_doc["type"]
                for ref_id in refs:
                    relationship_triples.add((doc_id, slot, ref_id))

    context.log.info(
        f"Found {len(id_to_type_map)} documents, with "
        f"{len({d for (d, _, _) in relationship_triples})} containing references"
    )

    # The bifurcation of document-reference-ranged slots as "upstream" and "downstream" is essential
    # in order to perform graph traversal and collect all entities "related" to a given entity without
    # recursion "exploding".
    #
    # Note: We are hard-coding this "direction" information here in the Runtime
    #       because the NMDC schema does not currently contain or expose it.
    #
    # An "upstream" slot is such that the range entity originated, or helped produce, the domain entity.
    upstream_document_reference_ranged_slots = [
        "associated_studies",  # when a `nmdc:Study` is upstream of a `nmdc:Biosample`.
        "collected_from",  # when a `nmdc:Site` is upstream of a `nmdc:Biosample`.
        "expected_organism",  # when an `nmdc:Organism` is upstream of the `nmdc:OrganismSample` someone expects to contain it.
        "has_chromatography_configuration",  # when a `nmdc:Configuration` is upstream of a `nmdc:PlannedProcess`.
        "has_input",  # when a `nmdc:NamedThing` is upstream of a `nmdc:PlannedProcess`.
        "has_mass_spectrometry_configuration",  # when a `nmdc:Configuration` is upstream of a `nmdc:PlannedProcess`.
        "instrument_used",  # when a `nmdc:Instrument` is upstream of a `nmdc:PlannedProcess`.
        "part_of",  # when a `nmdc:NamedThing` is upstream of a `nmdc:NamedThing`.
        "was_generated_by",  # when a `nmdc:DataEmitterProcess` is upstream of a `nmdc:DataObject`.
        "was_informed_by",  # when a  `nmdc:DataGeneration` is upstream of a `nmdc:WorkflowExecution`.
    ]
    # A "downstream" slot is such that the range entity originated from, or is considered part of, the domain entity.
    downstream_document_reference_ranged_slots = [
        "calibration_object",  # when a `nmdc:DataObject` is downstream of a `nmdc:CalibrationInformation`.
        "generates_calibration",  # when a `nmdc:CalibrationInformation` is downstream of a `nmdc:PlannedProcess`.
        "has_output",  # when a `nmdc:NamedThing` is downstream of a `nmdc:PlannedProcess`.
        "in_manifest",  # when a `nmdc:Manifest` is downstream of a `nmdc:DataObject`.
        "uses_calibration",  # when a `nmdc:CalibrationInformation`is part of a `nmdc:PlannedProcess`.
        # Note: I don't think of superseding something as being either upstream or downstream of that thing;
        #       but this function requires every document-reference-ranged slot to be accounted for in one
        #       list or the other, and the superseding thing does arise _later_ than the thing it supersedes,
        #       so I have opted to treat the superseding thing as being downstream.
        "superseded_by",  # when a `nmdc:WorkflowExecution` or `nmdc:DataObject` is superseded by a `nmdc:WorkflowExecution`.
    ]

    unique_document_reference_ranged_slot_names = set()
    for slot_names in document_reference_ranged_slots_by_type.values():
        for slot_name in slot_names:
            unique_document_reference_ranged_slot_names.add(slot_name)
    context.log.info(f"{unique_document_reference_ranged_slot_names=}")
    unclassified_slot_names = (
        unique_document_reference_ranged_slot_names
        - set(upstream_document_reference_ranged_slots)
        - set(downstream_document_reference_ranged_slots)
    )
    if unclassified_slot_names:
        raise Failure(
            "Encountered document-reference-ranged slot(s) with no upstream/downstream "
            f"classification: {sorted(unclassified_slot_names)}"
        )

    # Construct, and update documents with, `_upstream` and `_downstream` field values.
    #
    # manage batching of MongoDB `bulk_write` operations
    bulk_operations, update_count = [], 0
    for doc_id, slot, ref_id in relationship_triples:

        # Determine in which respective fields to push this relationship
        # for the subject (doc) and object (ref) of this triple.
        if slot in upstream_document_reference_ranged_slots:
            field_for_doc, field_for_ref = "_upstream", "_downstream"
        elif slot in downstream_document_reference_ranged_slots:
            field_for_doc, field_for_ref = "_downstream", "_upstream"
        else:
            raise Failure(f"Unknown slot {slot} for document {doc_id}")

        updates = [
            {
                "filter": {"id": doc_id},
                "update": {
                    "$push": {
                        field_for_doc: {
                            "id": ref_id,
                            # TODO existing tests are failing due to `KeyError`s for `id_to_type_map.get[ref_id]` here,
                            #   which acts as an implicit referential integrity checker (!). Using `.get` with
                            #   "nmdc:NamedThing" as default in order to (for now) allow such tests to continue to pass.
                            "type": id_to_type_map.get(ref_id, "nmdc:NamedThing"),
                        }
                    }
                },
            },
            {
                "filter": {"id": ref_id},
                "update": {
                    "$push": {
                        field_for_ref: {"id": doc_id, "type": id_to_type_map[doc_id]}
                    }
                },
            },
        ]
        for update in updates:
            bulk_operations.append(UpdateOne(**update))

        # Execute in batches for efficiency
        if len(bulk_operations) >= BULK_WRITE_BATCH_SIZE:
            temp_collection.bulk_write(bulk_operations)
            update_count += len(bulk_operations)
            context.log.info(
                f"Pushed {update_count/(2*len(relationship_triples)):.1%} of updates so far..."
            )
            bulk_operations = []

    # Execute any remaining operations
    if bulk_operations:
        temp_collection.bulk_write(bulk_operations)
        update_count += len(bulk_operations)

    context.log.info(f"Pushed {update_count} updates in total")


def drop_temporary_alldocs_collections(
    mdb: MongoDatabase,
    temporary_alldocs_collection_name_prefix: str = "_runtime.tmp.alldocs.",
) -> tuple[int, int]:
    """
    Drops all temporary alldocs collections.

    :returns: Tuple of two numbers. First number is number of collections that were found,
              and second number is number of collections that were dropped.
    """

    num_collections_initial = 0
    num_collections_dropped = 0

    for collection_name in mdb.list_collection_names():
        num_collections_initial += 1

        # If this collection's name begins with the specified prefix, drop the collection.
        if collection_name.startswith(temporary_alldocs_collection_name_prefix):
            num_collections_dropped += 1
            mdb.drop_collection(collection_name)

    return (num_collections_initial, num_collections_dropped)


# Note: Here, we define a so-called "Nothing dependency," which allows us to (in a graph)
#       pass an argument to the op (in order to specify the order of the ops in the graph)
#       while also telling Dagster that this op doesn't need the _value_ of that argument.
#       This is the approach shown on: https://docs.dagster.io/api/dagster/types#dagster.Nothing
#       Reference: https://docs.dagster.io/guides/build/ops/graphs#defining-nothing-dependencies
#
@op(required_resource_keys={"mongo"}, ins={"waits_for": In(dagster_type=Nothing)})
def materialize_alldocs(context: OpExecutionContext) -> int:
    """
    This function (re)builds the `alldocs` collection to reflect the current state of the MongoDB database by:

    1. Getting all populated schema collection names with an `id` field.
    2. Create a temporary collection to build the new alldocs collection.
    3. For each document in schema collections, extract `id`, `type`, and document-reference-ranged slot values.
    4. Add a special `_type_and_ancestors` field that contains the class hierarchy for the document's type.
    5. Add special `_upstream` and `_downstream` fields with subdocuments containing ID and type of related entities.
    6. Add indexes for `id`, relationship fields, and `{_upstream,_downstream}{.id,(.type, .id)}` (compound) indexes.
    7. Finally, atomically replace the existing `alldocs` collection with the temporary one.

    The `alldocs` collection is scheduled to be updated hourly via a scheduled job defined as
    `nmdc_runtime.site.repository.ensure_alldocs_hourly`.

    The `alldocs` collection is used primarily by API endpoints like `/data_objects/study/{study_id}` and
    `/workflow_executions/{workflow_execution_id}/related_resources` that need to perform graph traversal to find
    related documents. It serves as a denormalized view of the database to make these complex queries more efficient.

    The {`_upstream`,`_downstream`} fields enable efficient index-covered queries to find all entities of specific types
    that are related to a given set of source entities, leveraging the `_type_and_ancestors` field for subtype
    expansions.
    """
    mdb = context.resources.mongo.db
    schema_view = nmdc_schema_view()

    # TODO include functional_annotation_agg  for "real-time" ref integrity checking.
    #   For now, production use cases for materialized `alldocs` are limited to `id`-having collections.
    collection_names = populated_schema_collection_names_with_id_field(mdb)
    context.log.info(f"constructing `alldocs` collection using {collection_names=}")

    document_class_names = set(
        chain.from_iterable(collection_name_to_class_names.values())
    )

    cls_slot_map = {
        cls_name: {
            slot.name: slot for slot in schema_view.class_induced_slots(cls_name)
        }
        for cls_name in document_class_names
    }

    # Any ancestor of a document class is a document-referenceable range,
    # i.e., a valid range of a document-reference-ranged slot.
    document_referenceable_ranges = set(
        chain.from_iterable(
            schema_view.class_ancestors(cls_name) for cls_name in document_class_names
        )
    )

    document_reference_ranged_slots_by_type = defaultdict(list)
    for cls_name, slot_map in cls_slot_map.items():
        for slot_name, slot in slot_map.items():
            if (
                set(get_names_of_classes_in_effective_range_of_slot(schema_view, slot))
                & document_referenceable_ranges
            ):
                document_reference_ranged_slots_by_type[f"nmdc:{cls_name}"].append(
                    slot_name
                )

    # Before we generate a temporary `_runtime.tmp.alldocs.*` collection, drop any such collections
    # left over from previous generation attempts that failed. This prevents the database from
    # accumulating too many such collections as generation attempts fail over time.
    temporary_alldocs_collection_name_prefix = "_runtime.tmp.alldocs."
    _, num_dropped = drop_temporary_alldocs_collections(
        mdb=mdb,
        temporary_alldocs_collection_name_prefix=temporary_alldocs_collection_name_prefix,
    )
    context.log.info(f"Dropped {num_dropped} collections from past attempts")

    # Build `alldocs` to a temporary collection for atomic replacement
    # https://www.mongodb.com/docs/v6.0/reference/method/db.collection.renameCollection/#resource-locking-in-replica-sets
    temp_alldocs_collection_name = (
        f"{temporary_alldocs_collection_name_prefix}{ObjectId()}"
    )
    temp_alldocs_collection = mdb[temp_alldocs_collection_name]
    context.log.info(f"constructing `{temp_alldocs_collection.name}` collection")

    for coll_name in collection_names:
        context.log.info(f"{coll_name=}")
        write_operations = []
        documents_processed_counter = 0
        for doc in mdb[coll_name].find():
            try:
                # Keep the full type with prefix for document
                doc_type_full = doc["type"]
                # Remove prefix for slot lookup and ancestor lookup
                doc_type = doc_type_full.removeprefix("nmdc:")
            except KeyError:
                raise Exception(
                    f"doc {doc['id']} in collection {coll_name} has no 'type'!"
                )
            slots_to_include = ["id", "type"] + document_reference_ranged_slots_by_type[
                doc_type_full
            ]
            new_doc = keyfilter(lambda slot: slot in slots_to_include, doc)

            # Get ancestors without the prefix, but add prefix to each one in the output
            new_doc["_type_and_ancestors"] = [
                f"nmdc:{a}" for a in schema_view.class_ancestors(doc_type)
            ]
            # InsertOne is a pymongo representation of a mongo command.
            write_operations.append(InsertOne(new_doc))
            if len(write_operations) == BULK_WRITE_BATCH_SIZE:
                _ = temp_alldocs_collection.bulk_write(write_operations, ordered=False)
                write_operations.clear()
                documents_processed_counter += BULK_WRITE_BATCH_SIZE
        if len(write_operations) > 0:
            # here bulk_write is a method on the pymongo db Collection class
            _ = temp_alldocs_collection.bulk_write(write_operations, ordered=False)
            documents_processed_counter += len(write_operations)
        context.log.info(
            f"Inserted {documents_processed_counter} documents from {coll_name=} "
        )

    context.log.info(
        f"produced `{temp_alldocs_collection.name}` collection with"
        f" {temp_alldocs_collection.estimated_document_count()} docs."
    )

    context.log.info(f"creating indexes on `{temp_alldocs_collection.name}` ...")
    # Ensure unique index on "id". Index creation here is blocking (i.e. background=False),
    # so that `temp_alldocs_collection` will be "good to go" on renaming.
    temp_alldocs_collection.create_index("id", unique=True)
    # Add indexes to improve performance of `GET /data_objects/study/{study_id}`:
    slots_to_index = {"_type_and_ancestors"} | {
        slot
        for slots in document_reference_ranged_slots_by_type.values()
        for slot in slots
    }
    [temp_alldocs_collection.create_index(slot) for slot in slots_to_index]
    context.log.info(f"created indexes on id and on each of {slots_to_index=}.")

    # Add related-ids fields to enable efficient relationship traversal
    context.log.info("Adding fields for related ids to documents...")
    _add_linked_instances_to_alldocs(
        temp_alldocs_collection, context, document_reference_ranged_slots_by_type
    )
    context.log.info("Creating {`_upstream`,`_downstream`} indexes...")
    temp_alldocs_collection.create_index("_upstream.id")
    temp_alldocs_collection.create_index("_downstream.id")
    # Create compound indexes to ensure index-covered queries
    temp_alldocs_collection.create_index([("_upstream.type", 1), ("_upstream.id", 1)])
    temp_alldocs_collection.create_index(
        [("_downstream.type", 1), ("_downstream.id", 1)]
    )
    context.log.info("Successfully created {`_upstream`,`_downstream`} indexes")

    context.log.info(f"renaming `{temp_alldocs_collection.name}` to `alldocs`...")
    temp_alldocs_collection.rename("alldocs", dropTarget=True)
    n_alldocs_documents = mdb.alldocs.estimated_document_count()
    context.log.info(
        f"Rebuilt `alldocs` collection with {n_alldocs_documents} documents."
    )
    return n_alldocs_documents
