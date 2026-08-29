"""Dagster ops for validating documents in the MongoDB database.

In Dagster...
- A run is an execution of a job.
- A job is an executable version of a graph. Docs: https://docs.dagster.io/api/dagster/jobs
- A graph is a composition of ops. Docs: https://docs.dagster.io/api/dagster/graphs
- An op is a single piece of work. Docs: https://docs.dagster.io/api/dagster/ops
"""

from dataclasses import asdict, dataclass
from typing import Any, Iterator

from dagster import (
    Array,
    AssetKey,
    AssetMaterialization,
    Failure,
    Field,
    MetadataValue,
    OpExecutionContext,
    Output,
    op,
)
from linkml_runtime import SchemaView
from linkml.validator import Validator
from linkml.validator.report import ValidationResult
from pymongo.database import Database as MongoDatabase
from refscan.lib.helpers import (
    get_collection_name_to_class_names_map,
    translate_class_uri_into_schema_class_name,
)

from nmdc_runtime.api.endpoints.util import strip_oid
from nmdc_runtime.site.ops.common import send_slack_message
from nmdc_runtime.util import get_nmdc_schema_validator, nmdc_schema_view


@dataclass
class CollectionValidationSummary:
    """Summary of the result of validating the documents in a single MongoDB collection."""

    collection_name: str
    """Name of the collection whose documents were validated."""

    eligible_class_names: list[str]
    """Names of NMDC Schema classes the schema says the collection can store [documents representing] instances of."""

    num_documents_checked: int = 0
    """Number of documents validated, regardless of the validation result. Will remain at 0 if collection was skipped."""

    num_documents_having_violations: int = 0
    """Number of documents having any validation errors, regardless of the number of errors."""

    num_violations: int = 0
    """Number of validation errors, regardless of the number of documents they were found in."""

    collection_was_skipped: bool = False
    """Whether [the documents in] this collection was actually skipped, instead of being validated."""


def get_document_identifier(document: dict[str, Any]) -> str:
    """
    Returns a human-readable identifier for the document.

    If the document has an `id` field (which documents in most—but not all—NMDC Schema-described
    collections do), use the `id` value as the document's label; otherwise, use its `_id` value.

    >>> get_document_identifier({"_id": 1, "id": 2})
    '2'

    >>> get_document_identifier({"_id": 1})
    '1'

    >>> get_document_identifier({})
    Traceback (most recent call last):
      ...
    KeyError: 'Failed to get identifier for document: {}'
    """

    if "id" in document:
        return str(document["id"])
    elif "_id" in document:
        return str(document["_id"])
    else:
        raise KeyError(f"Failed to get identifier for document: {document}")


def _validate_collection(
    context: OpExecutionContext,
    *,
    mongo_database: MongoDatabase,
    schema_view: SchemaView,
    validator: Validator,
    collection_name: str,
    eligible_class_names: list[str],
) -> CollectionValidationSummary:
    """
    Validate all documents in the specified MongoDB collection and return a summary of the
    validation result.
    """

    # Initialize the validation result summary.
    summary = CollectionValidationSummary(
        collection_name=collection_name,
        eligible_class_names=eligible_class_names,
        num_documents_checked=0,
        num_documents_having_violations=0,
        num_violations=0,
        collection_was_skipped=False,
    )

    # Get a MongoDB cursor that we can use to iterate over all documents in the collection.
    #
    # Note: By default, MongoDB fetches up to 101 documents at a time, or 16 MiB worth of documents
    #       at a time (whichever is smaller). Since 16 MiB / 101 = 0.16 MiB, and I think it is quite
    #       common for our collections to have documents even smaller than that, I set the batch
    #       size to something larger here. Even with a custom batch size, MongoDB still applies the
    #       rule in the first sentence, just with our custom number instead of 101. By the way, the
    #       advantage of using fewer batches is that, for each batch, pymongo has to do a network
    #       round trip with the MongoDB server.
    #       Docs: https://www.mongodb.com/docs/v8.0/reference/method/cursor.batchSize
    #
    #       We can tune the batch size over time. As of today, a document in the huge
    #       `functional_annotation_agg` collection contains about 200 JSON characters.
    #       Estimating that to be 200 bytes, I initialized the batch size to be 80_000,
    #       since 80,000 x 200 = 16,000,000.
    #
    cursor = mongo_database[collection_name].find({}, batch_size=80_000)
    for document in cursor:
        summary.num_documents_checked += 1

        # Determine the name of the schema class this document represents an instance of.
        document_type = document.get("type", None)
        document_schema_class_name = (
            translate_class_uri_into_schema_class_name(schema_view, document_type)
            if isinstance(document_type, str)
            else None
        )

        # If the schema says this collection cannot store documents representing instances of that
        # schema class, record a violation and stop validating this document.
        eligible_schema_class_names = set(eligible_class_names)
        if document_schema_class_name not in eligible_schema_class_names:
            summary.num_documents_having_violations += 1
            summary.num_violations += 1
            context.log.error(
                "Validation error: In collection '%s', document '%s' has type %r, which does not "
                "map to any schema classes compatible with that collection (those are: %s).",
                collection_name,
                get_document_identifier(document),
                document_type,
                sorted(eligible_class_names),
            )

            # Stop validating this document and move on to the next one.
            continue

        # Validate the document (without its `_id` field) against the NMDC Schema and get a list
        # of the validation results.
        #
        # Note: Even though we already know the schema class this document represents, we can't take
        #       advantage of that knowledge with the `target_class` kwarg here. The `Validator` we
        #       are using is configured to use the `JsonschemaValidationPlugin` plugin, and that
        #       plugin is configured with a `json_schema_path` value, and when the latter is true,
        #       the validator ignores the `target_class`; instead, it uses schema's top-level class,
        #       which is "Database")! So, here, we effectively "wrap" our document in a "Database".
        #       Reference: https://github.com/linkml/linkml/blob/24e73c9a007e57a8df28964784d7feb247b82983/packages/linkml/src/linkml/validator/validation_context.py#L81-L96
        #
        validation_results: list[ValidationResult] = list(
            validator.iter_results(instance={collection_name: [strip_oid(document)]})
        )

        # If there were any validation results, report each one as a validation error coming from
        # this document.
        if len(validation_results) > 0:
            summary.num_documents_having_violations += 1
            summary.num_violations += len(validation_results)
            for validation_result in validation_results:
                context.log.error(
                    "Validation error: In collection '%s', document '%s' ('%s'): [%s/%s] %s%s",
                    collection_name,
                    get_document_identifier(document),
                    document_schema_class_name,
                    validation_result.severity,
                    validation_result.type,
                    validation_result.message,
                    (
                        f"; context={validation_result.context}"
                        if validation_result.context
                        else ""
                    ),
                )

    # Now that we've checked all of the documents in this collection, return a summary of the
    # validation results.
    return summary


def _make_validation_result_asset_materialization_event(
    collection_validation_summary: CollectionValidationSummary,
) -> AssetMaterialization:
    """
    Create a Dagster event indicating that an op has materialized a report that summarizes
    a collection's validation result.

    In Dagster...
    - An asset is an object in persistent storage. Docs: https://docs.dagster.io/api/dagster/assets
    - An `AssetMaterialization` is an event used to report that an asset has been materialized.
    - An `AssetKey` gives the asset a name that users can find on the Dagster web UI.

    Docs: https://docs.dagster.io/guides/build/ops/op-events#asset-materializations
    """

    cvs = collection_validation_summary  # concise alias

    return AssetMaterialization(
        # Example resulting asset_key: `validation/study_set_validation_result`
        asset_key=AssetKey(["validation", f"{cvs.collection_name}_validation_result"]),
        description=(
            "NMDC Schema-based validation of the documents "
            f"in the MongoDB collection named: {cvs.collection_name}"
        ),
        metadata={
            "eligible_class_names": MetadataValue.text(
                ", ".join(cvs.eligible_class_names)
            ),
            "num_documents_checked": MetadataValue.int(cvs.num_documents_checked),
            "num_documents_having_violations": MetadataValue.int(
                cvs.num_documents_having_violations
            ),
            "num_violations": MetadataValue.int(cvs.num_violations),
            "collection_was_skipped": MetadataValue.bool(cvs.collection_was_skipped),
        },
    )


def select_collection_names(
    eligible_collection_names: set[str],
    included_collection_names: set[str],
    excluded_collection_names: set[str],
) -> set[str]:
    """
    Determine the names of the collections whose documents we will validate; considering any names
    specified by the user (or whatever launched this run; e.g. Dagster's scheduler).

    By default, all eligible collection names will become the baseline set. If the "include" list is
    non-empty, however, then _that_ will be the baseline. The "exclude" list will be applied to the
    baseline. See the doctests below for an illustration of this.

    >>> sorted(select_collection_names(
    ...     eligible_collection_names={"a", "b", "c", "d", "e"},
    ...     included_collection_names=set(),
    ...     excluded_collection_names=set(),
    ... ))
    ['a', 'b', 'c', 'd', 'e']

    >>> sorted(select_collection_names(
    ...     eligible_collection_names={"a", "b", "c", "d", "e"},
    ...     included_collection_names={"a", "c"},
    ...     excluded_collection_names=set(),
    ... ))
    ['a', 'c']

    >>> sorted(select_collection_names(
    ...     eligible_collection_names={"a", "b", "c", "d", "e"},
    ...     included_collection_names={"a", "c"},
    ...     excluded_collection_names={"e"},
    ... ))
    ['a', 'c']

    >>> sorted(select_collection_names(
    ...     eligible_collection_names={"a", "b", "c", "d", "e"},
    ...     included_collection_names=set(),
    ...     excluded_collection_names={"c", "e"},
    ... ))
    ['a', 'b', 'd']
    """
    # First, we validate the "exclude" and "include" lists. If either list contains the name of any
    # collections that isn't in the "eligible" list, or the same name appears in both the "exclude"
    # and the "include" lists, abort with an error.
    invalid_excluded_collection_names = excluded_collection_names.difference(
        eligible_collection_names
    )
    if invalid_excluded_collection_names != set():
        raise Failure(
            "The following collections are not eligible for exclusion: "
            f"{invalid_excluded_collection_names}"
        )
    invalid_included_collection_names = included_collection_names.difference(
        eligible_collection_names
    )
    if invalid_included_collection_names != set():
        raise Failure(
            "The following collections are not eligible for inclusion: "
            f"{invalid_included_collection_names}"
        )
    contradictory_collection_names = excluded_collection_names.intersection(
        included_collection_names
    )
    if contradictory_collection_names != set():
        raise Failure(
            "A collection cannot be specified for both inclusion and exclusion simultaneously. "
            f"The problematic collection names are: {contradictory_collection_names}"
        )

    # Now that we've validated the "exclude" and "include" lists, utilize them.
    selected_collection_names = eligible_collection_names
    if included_collection_names == set():
        selected_collection_names = selected_collection_names.difference(
            excluded_collection_names
        )
    else:
        selected_collection_names = included_collection_names.difference(
            excluded_collection_names
        )
    return selected_collection_names


@op(
    required_resource_keys={"mongo", "slack_resource"},
    # Note: Users can specify `exclude_collections` and `include_collections` via the "Launchpad"
    #       tab on the Dagster web UI.
    config_schema={
        "exclude_collections": Field(
            Array(str),
            default_value=[],
            description=("Do not validate these collections."),
        ),
        "include_collections": Field(
            Array(str),
            default_value=[],
            description=(
                "When empty, all collections will be validated. "
                "When non-empty, only these collections will be validated. "
                "Exclusion will be performed after inclusion."
            ),
        ),
    },
)
def validate_mongo_data_op(
    context: OpExecutionContext,
) -> Iterator[AssetMaterialization | Output]:
    """
    Validate every document in every NMDC Schema-described MongoDB collection
    (e.g. "study_set", "biosample_set", etc., but not "users", "jobs", etc.).

    Note: We appended "_op" to this function's name because Dagster says:
          > Op/Graph definition names must be unique within a repository.
          And we already named a graph "validate_mongo_data".
    """

    # Get the names of all NMDC Schema-described collections; and determine which schema classes the
    # schema says each collection can store [documents representing] instances of.
    schema_view = nmdc_schema_view()
    class_names_by_collection_name = get_collection_name_to_class_names_map(
        schema_view=schema_view,
    )
    collection_names_from_schema = sorted(class_names_by_collection_name.keys())

    # Get a validator bound to the NMDC Schema.
    validator = get_nmdc_schema_validator()

    # Determine which collections we will validate, based upon the configuration of this op
    # for this run.
    excluded_collection_names = set(context.op_config["exclude_collections"])
    included_collection_names = set(context.op_config["include_collections"])
    selected_collection_names = select_collection_names(
        eligible_collection_names=set(collection_names_from_schema),
        included_collection_names=included_collection_names,
        excluded_collection_names=excluded_collection_names,
    )
    unselected_collection_names = set(collection_names_from_schema).difference(
        selected_collection_names
    )
    context.log.info(
        "Validating documents in %d of %d schema-described collections: %s.\n\n"
        "Skipping %d of %d schema-described collections: %s",
        len(selected_collection_names),
        len(collection_names_from_schema),
        sorted(selected_collection_names),
        len(unselected_collection_names),
        len(collection_names_from_schema),
        sorted(unselected_collection_names),
    )

    # Initialize a mapping from collection name to validation summary (for that collection).
    validation_summaries_by_collection_name: dict[str, CollectionValidationSummary] = {}

    # Process each of the selected collections.
    sorted_selected_collection_names = sorted(selected_collection_names)
    for collection_number, collection_name in enumerate(
        sorted_selected_collection_names, start=1
    ):
        class_names = sorted(class_names_by_collection_name[collection_name])
        context.log.info(
            "Validating collection %d of %d: '%s'\n(%d eligible %s: %s)",
            collection_number,
            len(selected_collection_names),
            collection_name,
            len(class_names),
            "class" if len(class_names) == 1 else "classes",
            class_names,
        )

        # Use a helper function to validate the documents in this collection.
        validation_summary = _validate_collection(
            context,
            mongo_database=context.resources.mongo.db,
            schema_view=schema_view,
            validator=validator,
            collection_name=collection_name,
            eligible_class_names=class_names,
        )
        validation_summaries_by_collection_name[collection_name] = validation_summary

        # Report an `AssetMaterialization` event to Dagster.
        #
        # Note: We use `yield` to report this event to Dagster without completely `return`-ing from
        #       this function. This way, users can see results (for this collection) without waiting
        #       for the entire job to finish.
        #
        # Docs: https://docs.dagster.io/guides/build/ops/op-events#asset-materializations
        #
        event = _make_validation_result_asset_materialization_event(validation_summary)
        yield event

    # Make a report that accounts for all validation performed.
    overall_report: dict[str, dict] = {}
    for (
        collection_name,
        validation_summary,
    ) in validation_summaries_by_collection_name.items():
        overall_report[collection_name] = asdict(
            validation_summary
        )  # DataClass instance -> dict

    # Count the total number of validation errors across all documents in all selected collections.
    # If there are any, we fail the run.
    #
    # Note: A document can have multiple validation errors (e.g. 2 invalid fields). Here, we get the
    #       total number of validation errors across all documents, as opposed to just the number of
    #       documents that had errors.
    #
    total_validation_errors = sum(
        validation_summary.num_violations
        for validation_summary in validation_summaries_by_collection_name.values()
    )
    if total_validation_errors > 0:
        raise Failure(
            f"We detected {total_validation_errors} validation errors.",
            metadata={
                "validation_errors": total_validation_errors,
                "collection_results": MetadataValue.json(overall_report),
            },
        )

    send_slack_message(
        context=context,
        text=r"Finished validating data in MongoDB database.",
        raise_on_error=False,
    )

    # Finally, we yield the overall report.
    #
    # Note: The Dagster docs say we have to `yield` it because we are also `yield`-ing events from
    #       within this op. Docs: https://docs.dagster.io/api/dagster/ops#dagster.Output
    #
    # Note: We only get to this point if we didn't `raise Failure(...)` somewhere above.
    #
    yield Output(MetadataValue.json(overall_report))
