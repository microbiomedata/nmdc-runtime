"""
Dagster ops related to the MongoDB collection named "alldocs".

Note: These were extracted from a 1900-line file at `nmdc_runtime/site/ops.py` during a refactor.
"""

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
        # Example: "nmdc:Study"
        doc_type = doc["type"]

        # Record ID to type mapping - preserve the original type with prefix
        # Example: array["nmdc:sty-00-000001"] = "nmdc:Study"
        id_to_type_map[doc_id] = doc_type

        # Find all document references from this document
        # Example: ["part_of"]
        reference_slots = document_reference_ranged_slots_by_type.get(doc_type, [])
        for slot in reference_slots:
            if slot in doc:
                # Handle both single-value and array references
                # Example: refs = ["nmdc:sty-00-parent", "nmdc:sty-00-otherparent"]
                refs = doc[slot] if isinstance(doc[slot], list) else [doc[slot]]
                for ref_id in refs:
                    relationship_triples.add((doc_id, slot, ref_id))

    context.log.info(
        f"Found {len(id_to_type_map)} documents, "
        f"{len({d for (d, _, _) in relationship_triples})} of which contain references."
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
    context.log.debug(f"{unique_document_reference_ranged_slot_names=}")
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

    # Note: We use `set` so the membership checks below are more efficient (than with `list`).
    upstream_slots = set(upstream_document_reference_ranged_slots)
    downstream_slots = set(downstream_document_reference_ranged_slots)

    # Populate the `links_by_doc_id` dictionary, whose first-level key is the `id` of the referring
    # document and whose second-level key if the name of the referring field of that document,
    # and whose second-level value is a list of dictionaries; each of which indicates the `id` and
    # `type` of the referenced document.
    links_by_doc_id: dict[str, dict[str, list[dict[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for source_document_id, slot_name, target_document_id in relationship_triples:
        # Determine which fields to record this relationship in, on the "subject" (i.e. source document)
        # and the "object" (i.e. target document) of this relationship triple.
        if slot_name in upstream_slots:
            # Example: WorkflowExecution ("_upstream" ->) --has_input--> (<- "_downstream") DataObject
            source_document_field_name, target_document_field_name = (
                "_upstream",
                "_downstream",
            )
        elif slot_name in downstream_slots:
            # Example: WorkflowExecution ("_downstream" ->) --has_output--> (<- "_upstream") DataObject
            source_document_field_name, target_document_field_name = (
                "_downstream",
                "_upstream",
            )
        else:
            raise Failure(f"Unknown slot {slot_name} for document {source_document_id}")

        # Note: If we encounter a target document whose `id` is not a key in the `id_to_type_map`
        #       dictionary, we raise an error. This implies a referential integrity violation.
        if target_document_id not in id_to_type_map.keys():
            raise Failure(
                f"'id_to_type_map' lacks key '{target_document_id}' "
                f"(encountered in field '{slot_name}' of document '{source_document_id}')."
            )
        links_by_doc_id[source_document_id][source_document_field_name].append(
            {
                "id": target_document_id,
                "type": id_to_type_map[target_document_id],
            }
        )
        links_by_doc_id[target_document_id][target_document_field_name].append(
            {
                "id": source_document_id,
                "type": id_to_type_map[source_document_id],
            }
        )

    # Count the total number of referring documents.
    total_updates = len(links_by_doc_id.keys())
    context.log.info(f"✅ Prepared to update {total_updates} referring documents.")

    # Submit one `UpdateOne` operation per referring document (a.k.a. "source" document).
    #
    # Note: In order to reduce the noise in the Dagster logs (e.g. "0.1%...", "0.8%..."), we only
    #       log progress when the progress reaches a new increment of 5% (e.g. "5%...", "10%...").
    #
    log_step_size: float = 0.05  # decimal representation of a percent (i.e. 5% -> 0.05)
    next_log_step: float = log_step_size
    bulk_operations, update_count = [], 0
    for source_document_id, fields_and_values in links_by_doc_id.items():
        bulk_operations.append(
            UpdateOne(
                filter={"id": source_document_id},
                update={"$set": dict(fields_and_values)},
            )
        )

        # If the number of operations we've prepared exceeds our threshold, write the batch to Mongo.
        #
        # Note: Since each source documents is only targeted by a single `UpdateOne` statement, we
        #       set `ordered=False` so Mongo doesn't waste any resources or sacrifice parallelization
        #       in order to perform the writes in any particular order.
        #       Docs: https://www.mongodb.com/docs/manual/core/bulk-write-operations/#ordered-vs-unordered-operations
        #
        if len(bulk_operations) >= BULK_WRITE_BATCH_SIZE:
            temp_collection.bulk_write(bulk_operations, ordered=False)
            update_count += len(bulk_operations)
            portion_updates_done = update_count / total_updates
            if portion_updates_done >= next_log_step:
                context.log.info(
                    f"Updated {portion_updates_done:.1%} of the "
                    f"{total_updates} referring documents so far."
                )
                while next_log_step <= portion_updates_done:
                    next_log_step = round(next_log_step + log_step_size, ndigits=3)
            bulk_operations = []

    # Execute any remaining operations.
    if bulk_operations:
        temp_collection.bulk_write(bulk_operations, ordered=False)
        update_count += len(bulk_operations)

    context.log.info(f"✅ Updated {update_count} documents in total.")


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
    3. For each document in schema collections, extract `id`, `type`, `qc_status`, and document-reference-ranged slot values.
    4. Add a special `_type_and_ancestors` field that contains the class hierarchy for the document's type.
    5. Add special `_upstream` and `_downstream` fields with subdocuments containing ID and type of related entities.
    6. Add indexes for `id`, `qc_status`, relationship fields, and `{_upstream,_downstream}{.id,(.type, .id)}` (compound) indexes.
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
    context.log.info(
        f"🧹 Dropped {num_dropped} collections from unfinished generation attempts.",
    )

    # Build `alldocs` to a temporary collection for atomic replacement
    # https://www.mongodb.com/docs/v6.0/reference/method/db.collection.renameCollection/#resource-locking-in-replica-sets
    temp_alldocs_collection_name = (
        f"{temporary_alldocs_collection_name_prefix}{ObjectId()}"
    )
    temp_alldocs_collection = mdb[temp_alldocs_collection_name]
    context.log.info(
        f"🗂️ Source collections ({len(collection_names)}): {collection_names}"
    )
    context.log.info(
        f"📂 Generating collection '{temp_alldocs_collection.name}' from source collections."
    )

    # Make a generic Mongo projection that includes all the fields that _could_ be relevant for any
    # document in any given collection, while omitting all the fields that could _not_. This is a
    # performance optimization that saves us from having to load the latter fields from Mongo into
    # the application unnecessarily (they would be discarded by the `keyfilter` below anyway).
    distinct_referring_field_names = set(
        chain.from_iterable(document_reference_ranged_slots_by_type.values())
    )
    projection = {"id": 1, "type": 1, "qc_status": 1}
    for name in distinct_referring_field_names:
        assert isinstance(name, str)
        projection[name] = 1

    for coll_name in collection_names:
        context.log.debug(f"Reading collection: {coll_name}")
        write_operations = []
        documents_processed_counter = 0
        for doc in mdb[coll_name].find({}, projection):
            try:
                # Keep the full type with prefix for document
                doc_type_full = doc["type"]
                # Remove prefix for slot lookup and ancestor lookup
                doc_type = doc_type_full.removeprefix("nmdc:")
            except KeyError:
                raise Exception(
                    f"doc {doc['id']} in collection {coll_name} has no 'type'!"
                )
            slots_to_include = [
                "id",
                "type",
                "qc_status",
            ] + document_reference_ranged_slots_by_type[doc_type_full]
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
            f"📄 Added {documents_processed_counter} documents from collection '{coll_name}'."
        )
    context.log.info(
        f"✅ Finished adding a total of {temp_alldocs_collection.count_documents({})} "
        f"documents to collection '{temp_alldocs_collection.name}'."
    )

    # Index the newly-populated collection.
    context.log.info(
        f"📖 Creating indexes on collection '{temp_alldocs_collection.name}'."
    )
    # Ensure unique index on "id". Index creation here is blocking (i.e. background=False),
    # so that `temp_alldocs_collection` will be "good to go" on renaming.
    temp_alldocs_collection.create_index("id", unique=True)
    slots_to_index = {"_type_and_ancestors", "qc_status"} | {
        slot
        for slots in document_reference_ranged_slots_by_type.values()
        for slot in slots
    }
    [temp_alldocs_collection.create_index(slot) for slot in slots_to_index]
    context.log.info("✅ Created unique index on field: 'id'")
    context.log.info(f"✅ Created indexes on fields: {slots_to_index}")

    # Add "_upstream" and "_downstream" fields to documents.
    context.log.info("🌊 Adding upstream/downstream traversal fields.")
    _add_linked_instances_to_alldocs(
        temp_alldocs_collection, context, document_reference_ranged_slots_by_type
    )
    context.log.info("📖 Creating indexes on upstream/downstream traversal fields.")
    temp_alldocs_collection.create_index("_upstream.id")
    temp_alldocs_collection.create_index("_downstream.id")
    # Create compound indexes to ensure index-covered queries
    temp_alldocs_collection.create_index([("_upstream.type", 1), ("_upstream.id", 1)])
    temp_alldocs_collection.create_index(
        [("_downstream.type", 1), ("_downstream.id", 1)]
    )
    context.log.info("✅ Created indexes on upstream/downstream traversal fields.")

    # Rename the temporary collection to "alldocs".
    context.log.info(
        f"🪪 Renaming collection '{temp_alldocs_collection.name}' to 'alldocs'."
    )
    temp_alldocs_collection.rename("alldocs", dropTarget=True)
    n_alldocs_documents = mdb.alldocs.count_documents({})
    context.log.info(
        "✅ Done materializing the `alldocs` collection, "
        f"which contains {n_alldocs_documents} documents."
    )
    return n_alldocs_documents
