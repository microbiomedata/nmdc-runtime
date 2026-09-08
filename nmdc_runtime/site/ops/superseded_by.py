"""
Dagster ops related to managing the "superseded_by" field of documents in the MongoDB collections
named "workflow_execution_set" and "data_object_set".
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable

from dagster import DagsterLogManager, OpExecutionContext, op
from pymongo import UpdateOne
from pymongo.database import Database

from nmdc_runtime.api.endpoints.lib.workflow_executions import (
    parse_workflow_execution_id,
)


class SentinelValue(Enum):
    """
    Value that cannot naturally occur in the context in which it is used.

    Reference: https://en.wikipedia.org/wiki/Sentinel_value
    """

    FIELD_ABSENT = auto()
    """Indicates that the field is absent from a MongoDB document."""

    NO_EXPECTATION = auto()
    """Indicates that we have no expectation for the field yet."""


@dataclass
class WorkflowExecutionDescriptor:
    """Encapsulates aspects of a `WorkflowExecution` that are related to the task at hand."""

    id: str
    """The `id` of the `WorkflowExecution` document."""

    run_number: int
    """The 'run number' derived from the `id` of the `WorkflowExecution` document."""

    has_output: set[str]
    """The set of distinct items in the `has_output` list of the `WorkflowExecution` document."""

    superseded_by: str | None | SentinelValue
    """The initial `superseded_by` value of the `WorkflowExecution` document."""

    superseded_by_expected: str | SentinelValue
    """The `superseded_by` value that correctly reflects the `WorkflowExecution` document's place in its supersession chain."""


def set_expectations_for_superseded_by_field(
    wfe_descriptors: list[WorkflowExecutionDescriptor],
) -> None:
    """
    Sorts the list of `WorkflowExecution` descriptors associated with a single "base ID" in place,
    and updates each descriptor's `superseded_by_expected` field to reflect our expectation
    about its `superseded_by` field.

    Preconditions:
    - All descriptors in the list have the same "base ID".
    - No two descriptors can have the same run number.

    - If there are no descriptors, do nothing (there is no supersession taking place).
    - If there is only one descriptor, expect it to not be superseded by anything.
    - Otherwise, expect each descriptor to be superseded by the next existing run in numeric order
      (gaps are allowed); except for the final run, which is not superseded by anything.
    - If multiple descriptors have the same run number, raise a `ValueError`.

    Define a helper for constructing example descriptors.
    >>> def make_wfe_descriptor(run_number):
    ...     return WorkflowExecutionDescriptor(
    ...         id=f"nmdc:wfe-00-001.{run_number}",
    ...         run_number=run_number,
    ...         has_output=set(),
    ...         superseded_by=SentinelValue.FIELD_ABSENT,
    ...         superseded_by_expected=SentinelValue.NO_EXPECTATION,
    ...     )

    1. If the list is empty, do nothing.
    >>> descriptors = []
    >>> set_expectations_for_superseded_by_field(descriptors)
    >>> descriptors
    []

    2. If there is one descriptor, expect its `superseded_by` field to be absent.
    >>> descriptors = [make_wfe_descriptor(1)]
    >>> set_expectations_for_superseded_by_field(descriptors)
    >>> descriptors[0].superseded_by_expected is SentinelValue.FIELD_ABSENT
    True

    3. Sort numerically and point `superseded_by` to the descriptor having the next existing run number (allowing for gaps).
    >>> descriptors = [make_wfe_descriptor(10), make_wfe_descriptor(2), make_wfe_descriptor(1)]
    >>> set_expectations_for_superseded_by_field(descriptors)
    >>> [descriptor.run_number for descriptor in descriptors]
    [1, 2, 10]
    >>> descriptors[0].superseded_by_expected
    'nmdc:wfe-00-001.2'
    >>> descriptors[1].superseded_by_expected
    'nmdc:wfe-00-001.10'
    >>> descriptors[2].superseded_by_expected is SentinelValue.FIELD_ABSENT
    True

    4. Raise an exception upon encountering a duplicate run number.
    >>> descriptors = [make_wfe_descriptor(2), make_wfe_descriptor(1), make_wfe_descriptor(2)]
    >>> set_expectations_for_superseded_by_field(descriptors)
    Traceback (most recent call last):
        ...
    ValueError: Workflow executions 'nmdc:wfe-00-001.2' and 'nmdc:wfe-00-001.2' have the same run number: 2.
    >>> all(descriptor.superseded_by_expected is SentinelValue.NO_EXPECTATION for descriptor in descriptors)
    True
    """

    # Sort the descriptors by run number (ascending).
    wfe_descriptors.sort(key=lambda descriptor: descriptor.run_number)

    # Check for duplicate run numbers and raise an exception if we encounter any.
    # Note: Since the descriptors are already sorted by run number, we just compare the
    #       current descriptor with and previous descriptor.
    for idx in range(1, len(wfe_descriptors)):
        previous_descriptor = wfe_descriptors[idx - 1]
        current_descriptor = wfe_descriptors[idx]
        if previous_descriptor.run_number == current_descriptor.run_number:
            raise ValueError(
                f"Workflow executions {previous_descriptor.id!r} and "
                f"{current_descriptor.id!r} have the same run number: "
                f"{current_descriptor.run_number}."
            )

    # Set the `superseded_by_expected` field to either the `id` of the descriptor having
    # the next existing run number; or to the sentinel value that indicates the field is absent.
    for idx, descriptor in enumerate(wfe_descriptors):
        if idx + 1 < len(wfe_descriptors):
            descriptor.superseded_by_expected = wfe_descriptors[idx + 1].id
        else:
            descriptor.superseded_by_expected = SentinelValue.FIELD_ABSENT

    # Return nothing, since we modified the input list in place.
    return None


def read_superseded_by_value(
    document: dict,
    warning_fn: Callable[[str], None] | None = None,
) -> str | None | SentinelValue:
    """
    Reads the `superseded_by` field of the specified document, returning one of the following
    normalized representations of it:
    1. If the field is missing, return `SentinelValue.FIELD_ABSENT`.
    2. If the value is `None`, return `None` and log a warning.
    3. If the value is a string, return that string.
    4. If the value is anything else, raise a `ValueError` exception.

    Define a mock warning function.
    >>> warnings = []
    >>> def warn(message):
    ...     warnings.append(message)

    1. If the field is missing, return `SentinelValue.FIELD_ABSENT`.
    >>> warnings = []
    >>> read_superseded_by_value({"id": "nmdc:wfe-00-000001.1"}, warn) is SentinelValue.FIELD_ABSENT
    True
    >>> len(warnings)
    0

    2. If the value is `None`, return `None` and log a warning.
    >>> warnings = []
    >>> read_superseded_by_value({"id": "nmdc:wfe-00-000001.1", "superseded_by": None}, warn) is None
    True
    >>> len(warnings)
    1

    3. If the value is a string, return that string.
    >>> warnings = []
    >>> read_superseded_by_value({"id": "nmdc:wfe-00-000001.1", "superseded_by": "nmdc:wfe-00-000001.2"}, warn)
    'nmdc:wfe-00-000001.2'
    >>> len(warnings)
    0

    4. If the value is anything else, raise a `ValueError` exception.
    >>> warnings = []
    >>> read_superseded_by_value({"id": "nmdc:wfe-00-000001.1", "superseded_by": False}, warn)
    Traceback (most recent call last):
        ...
    ValueError: Document 'nmdc:wfe-00-000001.1' has a `superseded_by` value of False, which does not comply with the NMDC schema.
    """

    if "superseded_by" not in document:
        return SentinelValue.FIELD_ABSENT

    raw_value = document["superseded_by"]
    if raw_value is None:
        if isinstance(warning_fn, Callable):
            warning_fn(
                f"Document {document.get('id')!r} has a `superseded_by` value "
                "of `None`, which violates NMDC conventions."
            )
        return None

    if isinstance(raw_value, str):
        return raw_value

    raise ValueError(
        f"Document {document.get('id')!r} has a `superseded_by` value "
        f"of {raw_value!r}, which does not comply with the NMDC schema."
    )


def read_has_output_value(
    document: dict,
    warning_fn: Callable[[str], None] | None = None,
) -> set[str]:
    """
    Reads the `has_output` field of the specified document, returning one of the following
    normalized representations of it:
    1. If the field is missing, return an empty set.
    2. If the value is `None`, return an empty set and log a warning.
    3. If the value is a list, return a set of its distinct items.
    4. If the value is anything else, raise a `ValueError` exception.

    Define a mock warning function.
    >>> warnings = []
    >>> def warn(message):
    ...     warnings.append(message)

    1. If the field is missing, return an empty set.
    >>> warnings = []
    >>> read_has_output_value({"id": "nmdc:wfe-00-000001.1"}, warn) == set()
    True
    >>> len(warnings)
    0

    2. If the value is `None`, return an empty set and log a warning.
    >>> warnings = []
    >>> read_has_output_value({"id": "nmdc:wfe-00-000001.1", "has_output": None}, warn) == set()
    True
    >>> len(warnings)
    1

    3. If the value is a list, return a set of its distinct items.
    >>> warnings = []
    >>> read_has_output_value({"id": "nmdc:wfe-00-000001.1", "has_output": []}, warn) == set()
    True
    >>> read_has_output_value({"id": "nmdc:wfe-00-000001.1", "has_output": ["a", "b"]}, warn) == {"a", "b"}
    True
    >>> read_has_output_value({"id": "nmdc:wfe-00-000001.1", "has_output": ["a", "b", "a"]}, warn) == {"a", "b"}
    True
    >>> len(warnings)
    0

    4. If the value is anything else, raise a `ValueError` exception.
    >>> warnings = []
    >>> read_has_output_value({"id": "nmdc:wfe-00-000001.1", "has_output": "a"}, warn)
    Traceback (most recent call last):
        ...
    ValueError: Document 'nmdc:wfe-00-000001.1' has a `has_output` value of 'a', which does not comply with the NMDC schema.
    """

    if "has_output" not in document:
        return set()

    raw_value = document["has_output"]
    if raw_value is None:
        if isinstance(warning_fn, Callable):
            warning_fn(
                f"Document {document.get('id')!r} has a `has_output` value "
                "of `None`, which violates NMDC conventions."
            )
        return set()

    if isinstance(raw_value, list):
        return set(raw_value)

    raise ValueError(
        f"Document {document.get('id')!r} has a `has_output` value "
        f"of {raw_value!r}, which does not comply with the NMDC schema."
    )


def make_update_statement_if_necessary(
    document_id: str,
    superseded_by_observed: str | None | SentinelValue,
    superseded_by_expected: str | SentinelValue,
) -> UpdateOne | None:
    """
    Returns an `UpdateOne` statement that would make the `superseded_by` value of the specified
    MongoDB document meet the expectation of it; or returns `None` if it already meets the expectation.

    1. No changes necessary, since expectation matches observation:
    >>> make_update_statement_if_necessary("nmdc:wfe-00-01.1", SentinelValue.FIELD_ABSENT, SentinelValue.FIELD_ABSENT) is None
    True
    >>> make_update_statement_if_necessary("nmdc:wfe-00-01.1", "nmdc:wfe-00-01.2", "nmdc:wfe-00-01.2") is None
    True

    2. Drops the field, to conform to NMDC convention of omitting "null" fields.
    >>> make_update_statement_if_necessary("nmdc:wfe-00-01.1", None, SentinelValue.FIELD_ABSENT) == UpdateOne({'id': 'nmdc:wfe-00-01.1'}, {'$unset': {'superseded_by': 1}})
    True

    3. Sets the field's value to match the expectation.
    >>> make_update_statement_if_necessary("nmdc:wfe-00-01.1", None, "nmdc:wfe-00-01.2") == UpdateOne({'id': 'nmdc:wfe-00-01.1'}, {'$set': {'superseded_by': 'nmdc:wfe-00-01.2'}})
    True
    """

    if superseded_by_expected is SentinelValue.NO_EXPECTATION:
        raise ValueError(f"Missing expectation for document: {document_id!r}")

    if superseded_by_expected == superseded_by_observed:
        return None

    if superseded_by_expected is SentinelValue.FIELD_ABSENT:
        return UpdateOne(
            filter={"id": document_id},
            update={"$unset": {"superseded_by": 1}},
        )

    return UpdateOne(
        filter={"id": document_id},
        update={"$set": {"superseded_by": superseded_by_expected}},
    )


@op(required_resource_keys={"mongo"})
def synchronize_superseded_by_field_op(
    context: OpExecutionContext,
) -> None:
    """
    Synchronize the "superseded_by" field of documents in the "workflow_execution_set" collection,
    so they reflect the sequences represented by "base ID" and "run number" parts of those documents'
    "id" values, based on the "id" conventions established by the NMDC workflow management team members
    (i.e. run number "5" supersedes run number "4", run number "4" supersedes run number "2", etc.,
    acknowledging that the run numbers of a given base ID are not necessarily contiguous).

    Also, synchronize the "superseded_by" field of documents in the "data_object_set" collection so
    they match the "superseded_by" field of the "workflow_execution_set" document that identifies
    those "data_object_set" documents as outputs (via the "has_output" field). If the "data_object_set"
    document is not identified as an output of any "workflow_execution_set" document, drop the
    "superseded_by" field (if present) from the "data_object_set" document.
    """

    # Get references to relevant MongoDB collections via the op execution context.
    db: Database = context.resources.mongo.db
    workflow_execution_set = db.get_collection("workflow_execution_set")
    data_object_set = db.get_collection("data_object_set")

    # Get a reference to the Dagster log manager via the op execution context.
    # Docs: https://docs.dagster.io/api/dagster/loggers#dagster.DagsterLogManager
    log: DagsterLogManager = context.log

    # Initialize lists of updates that we will eventually perform on each MongoDB collection.
    workflow_execution_set_update_statements: list[UpdateOne] = []
    data_object_set_update_statements: list[UpdateOne] = []

    log.info(
        "Building LUT of all `WorkflowExecution` descriptors, "
        "grouped by the base portion of their `id` values."
    )
    wfe_descriptors_by_base_id: dict[str, list[WorkflowExecutionDescriptor]] = {}
    for workflow_execution in workflow_execution_set.find(
        filter={},
        projection=dict(_id=False, id=True, has_output=True, superseded_by=True),
        batch_size=2_000,
    ):
        workflow_execution_id = workflow_execution["id"]
        base_id, run_number = parse_workflow_execution_id(workflow_execution_id)
        if base_id not in wfe_descriptors_by_base_id.keys():
            wfe_descriptors_by_base_id[base_id] = (
                []
            )  # initialize list of descriptors for base ID
        if run_number is None:
            raise ValueError(
                f"`WorkflowExecution` {workflow_execution_id!r} has no run number."
            )
        has_output = read_has_output_value(
            document=workflow_execution,
            warning_fn=log.warning,
        )
        superseded_by = read_superseded_by_value(
            workflow_execution, warning_fn=log.warning
        )
        wfe_descriptor = WorkflowExecutionDescriptor(
            id=workflow_execution_id,
            run_number=run_number,
            has_output=has_output,
            superseded_by=superseded_by,
            superseded_by_expected=SentinelValue.NO_EXPECTATION,
        )
        wfe_descriptors_by_base_id[base_id].append(wfe_descriptor)

    # TODO: Consider waiting to generate the `UpdateOne` statements until we are ready to submit
    #       them to the Mongo database, since they will occupy Memory while they exist.
    log.info(
        "Determining expectations for `superseded_by` fields of all `WorkflowExecution`s, "
        "and generating `UpdateOne` statements necessary to fulfill them."
    )
    for sorted_wfe_descriptors in wfe_descriptors_by_base_id.values():
        set_expectations_for_superseded_by_field(
            wfe_descriptors=sorted_wfe_descriptors,
        )
        for wfe_descriptor in sorted_wfe_descriptors:
            # Generate and store an `UpdateOne` statement, if necessary.
            update_statement = make_update_statement_if_necessary(
                document_id=wfe_descriptor.id,
                superseded_by_observed=wfe_descriptor.superseded_by,
                superseded_by_expected=wfe_descriptor.superseded_by_expected,
            )
            if isinstance(update_statement, UpdateOne):
                log.debug(f"Generated `UpdateOne` statement: {update_statement!r}")
                workflow_execution_set_update_statements.append(update_statement)

    log.info(
        "Building LUT from each `WorkflowExecution`-outputted `DataObject.id` to "
        "the expected `superseded_by` value of the outputting `WorkflowExecution`."
    )
    wfe_expected_superseded_by_value_by_own_output_id: dict[
        str, str | SentinelValue
    ] = {}
    for sorted_wfe_descriptors in wfe_descriptors_by_base_id.values():
        for wfe_descriptor in sorted_wfe_descriptors:
            for data_object_id in wfe_descriptor.has_output:
                if (
                    data_object_id
                    in wfe_expected_superseded_by_value_by_own_output_id.keys()
                ):
                    raise ValueError(
                        f"`DataObject` {data_object_id!r} is identified as "
                        "an output of multiple `WorkflowExecution`s."
                    )
                wfe_expected_superseded_by_value_by_own_output_id[data_object_id] = (
                    wfe_descriptor.superseded_by_expected
                )

    log.info(
        "Determining expectations for `superseded_by` fields of all `DataObject`s, "
        "and generating `UpdateOne` statements necessary to fulfill them."
    )
    for data_object in data_object_set.find(
        filter={},
        projection=dict(_id=False, id=True, superseded_by=True),
        batch_size=2_000,
    ):
        data_object_id = data_object["id"]
        superseded_by = read_superseded_by_value(data_object, warning_fn=log.warning)

        # Form our expectation for the `superseded_by` field, based on our expectation for the
        # `superseded_by` field of the outputting `WorkflowExecution`, if any.
        superseded_by_expected = SentinelValue.FIELD_ABSENT
        if data_object_id in wfe_expected_superseded_by_value_by_own_output_id.keys():
            superseded_by_expected = wfe_expected_superseded_by_value_by_own_output_id[
                data_object_id
            ]

        # Generate and `UpdateOne` statement, if necessary.
        update_statement = make_update_statement_if_necessary(
            document_id=data_object_id,
            superseded_by_observed=superseded_by,
            superseded_by_expected=superseded_by_expected,
        )
        if update_statement is not None:
            log.debug(f"Generated `UpdateOne`: {update_statement!r}")
            data_object_set_update_statements.append(update_statement)

    log.info(
        "Number of `UpdateOne` statements generated for `workflow_execution_set` collection: "
        f"{len(workflow_execution_set_update_statements)}"
    )
    log.info(
        "Number of `UpdateOne` statements generated for `data_object_set` collection: "
        f"{len(data_object_set_update_statements)}"
    )

    # Apply the updates to the documents in the MongoDB collections, atomically via a transaction.
    log.info(
        "Starting MongoDB transaction to ensure all updates are performed atomically."
    )
    with db.client.start_session() as session:
        with session.start_transaction():
            for collection_name, update_statements in (
                ("workflow_execution_set", workflow_execution_set_update_statements),
                ("data_object_set", data_object_set_update_statements),
            ):
                log.info(f"Applying updates to collection: {collection_name}")
                collection = db.get_collection(collection_name)

                num_update_statements = len(update_statements)
                if num_update_statements == 0:
                    log.info("No updates to apply.")
                    continue  # note: calling `bulk_write` with no requests would raise an exception

                # Note: We use `collection.bulk_write` instead of `db.command` because the former
                #       raises `BulkWriteError` for failed writes, whereas the latter requires manual
                #       inspection of the result. The former also provides simpler results.
                bulk_write_result = collection.bulk_write(
                    requests=update_statements,
                    ordered=False,
                    comment="Dagster op synchronizing 'superseded_by' fields",
                    session=session,
                )
                log.info(
                    f"Number of documents matched: {bulk_write_result.matched_count}\n"
                    f"Number of documents modified: {bulk_write_result.modified_count}"
                )
                if bulk_write_result.matched_count < num_update_statements:
                    raise RuntimeError(
                        f"Aborting MongoDB transaction. Failed to find as many {collection_name!r} "
                        f"documents as we expected (expected {num_update_statements}, found "
                        f"{bulk_write_result.matched_count}), which implies that some target "
                        "documents have been deleted since we began making the update plan."
                    )
