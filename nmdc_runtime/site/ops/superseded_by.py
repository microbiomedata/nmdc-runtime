"""
Dagster ops related to managing the "superseded_by" field of documents in the MongoDB collections
named "workflow_execution_set" and "data_object_set".
"""

from dataclasses import dataclass
from enum import Enum, auto

from dagster import DagsterLogManager, OpExecutionContext, op
from pymongo import UpdateOne
from pymongo.database import Database

from nmdc_runtime.api.endpoints.lib.workflow_executions import (
    parse_workflow_execution_id,
)


class SentinelValue(Enum):
    """
    Value that cannot occur in the context in which it is used.

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

    has_output: list[str]
    """The `has_output` value of the `WorkflowExecution` document."""

    superseded_by: str | None | SentinelValue
    """The initial `superseded_by` value of the `WorkflowExecution` document."""

    superseded_by_expected: str | SentinelValue
    """The `superseded_by` value that correctly reflects the `WorkflowExecution` document's place in its supersession chain."""


@op(required_resource_keys={"mongo"})
def synchronize_superseded_by_field_op(
    context: OpExecutionContext,
) -> None:
    """
    Synchronize the "superseded_by" field of documents in the "workflow_execution_set" collection,
    so they reflect the sequences represented by "base ID" and "run  number" parts of those documents'
    "id" values, based on the "id" conventions established by the NMDC workflow management team members.

    Also, synchronize the "superseded_by" field of documents in the "data_object_set" collection so
    they match the "superseded_by" field of the "workflow_execution_set" document that identifies
    those "data_object_set" documents as outputs (via the "has_output" field).
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
    for doc in workflow_execution_set.find(
        filter={},
        projection=dict(_id=False, id=True, has_output=True, superseded_by=True),
        batch_size=2_000,
    ):
        doc_id = doc["id"]
        base_id, run_number = parse_workflow_execution_id(doc_id)
        if base_id not in wfe_descriptors_by_base_id.keys():
            wfe_descriptors_by_base_id[base_id] = (
                []
            )  # initialize list of descriptors for base ID
        if run_number is None:
            raise ValueError(f"`WorkflowExecution` {doc_id!r} has no run number.")
        if any(
            run_number == wfe_desc.run_number
            for wfe_desc in wfe_descriptors_by_base_id[base_id]
        ):
            raise ValueError(
                f"Multiple `WorkflowExecutions` have both base ID {base_id!r} "
                f"and run number {run_number!r}."
            )
        has_output: list[str] = doc["has_output"] if "has_output" in doc else []
        superseded_by = SentinelValue.FIELD_ABSENT
        if "superseded_by" in doc:
            if isinstance(doc["superseded_by"], str):
                superseded_by = doc["superseded_by"]
            elif doc["superseded_by"] is None:
                log.warning(
                    f"`WorkflowExecution` {doc_id!r} has a `superseded_by` value "
                    "of `None`, which violates NMDC conventions."
                )
                superseded_by = None
            else:
                raise ValueError(
                    f"`WorkflowExecution` {doc_id!r} has a `superseded_by` value "
                    f"of {doc['superseded_by']!r}, which violates the NMDC schema."
                )
        wfe_descriptor = WorkflowExecutionDescriptor(
            id=doc_id,
            run_number=run_number,
            has_output=has_output,
            superseded_by=superseded_by,
            superseded_by_expected=SentinelValue.NO_EXPECTATION,
        )
        wfe_descriptors_by_base_id[base_id].append(wfe_descriptor)

    log.info(
        "Sorting `WorkflowExecution` descriptors within each group, by run number."
    )
    for wfe_descriptors_for_base_id in wfe_descriptors_by_base_id.values():
        # Note: For sorting, is important that the run numbers be numbers (e.g. 2 < 10),
        #       not numeric strings (e.g. "2" > "10") or a mixture. Fortunately, that is
        #       enforced by `parse_workflow_execution_id` and our checks for `None` above.
        wfe_descriptors_for_base_id.sort(key=lambda wfe_desc: wfe_desc.run_number)

    # TODO: Consider waiting to generate the `UpdateOne` statements until we are ready to submit
    #       them to the Mongo database, since they will occupy Memory while they exist.
    log.info(
        "Determining expectations for `superseded_by` fields of `WorkflowExecution`s, "
        "and generating `UpdateOne` statements that would achieve them."
    )
    for base_id, sorted_wfe_descriptors in wfe_descriptors_by_base_id.items():
        num_descriptors = len(sorted_wfe_descriptors)
        for idx, wfe_descriptor in enumerate(sorted_wfe_descriptors):
            # Indicate our expectation regarding the `superseded_by` field: If there is a descriptor
            # after this one in the group, then that one supersedes this one. Otherwise, nothing
            # supersedes this one (i.e. this is the "terminal" one).
            if idx + 1 < num_descriptors:
                wfe_descriptor.superseded_by_expected = sorted_wfe_descriptors[
                    idx + 1
                ].id
            else:
                wfe_descriptor.superseded_by_expected = SentinelValue.FIELD_ABSENT

            # If the document's `superseded_by` field already matches our expectation, proceed to
            # the next descriptor (rather than generating any "no op" `UpdateOne` statements).
            if wfe_descriptor.superseded_by == wfe_descriptor.superseded_by_expected:
                continue

            # Generate an `UpdateOne` statement that would make the `superseded_by` field
            # achieve our expectation of it (by either setting or unsetting the field).
            update = {"$set": {"superseded_by": wfe_descriptor.superseded_by_expected}}
            if wfe_descriptor.superseded_by_expected == SentinelValue.FIELD_ABSENT:
                update = {"$unset": {"superseded_by": 1}}
            workflow_update = UpdateOne(filter={"id": wfe_descriptor.id}, update=update)
            log.debug(f"Generated `UpdateOne` statement: {workflow_update!r}")
            workflow_execution_set_update_statements.append(workflow_update)

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
                        f"`DataObject` {data_object_id!r} is identified as an output of "
                        "multiple `WorkflowExecution`s."
                    )
                wfe_expected_superseded_by_value_by_own_output_id[data_object_id] = (
                    wfe_descriptor.superseded_by_expected
                )

    log.info(
        "Building LUT mapping all `DataObject` `id` values to "
        "those `DataObject`s' current `superseded_by` values."
    )
    dobj_superseded_by_map: dict[str, str | SentinelValue | None] = {}
    for doc in data_object_set.find(
        filter={},
        projection=dict(_id=False, id=True, superseded_by=True),
        batch_size=2_000,
    ):
        doc_id = doc["id"]
        superseded_by = SentinelValue.FIELD_ABSENT
        if "superseded_by" in doc:
            if isinstance(doc["superseded_by"], str):
                superseded_by = doc["superseded_by"]
            elif doc["superseded_by"] is None:
                log.warning(
                    f"`DataObject` {doc_id!r} has a `superseded_by` value "
                    "of `None`, which violates NMDC conventions."
                )
                superseded_by = None
            else:
                raise ValueError(
                    f"`DataObject` {doc_id!r} has a `superseded_by` value "
                    f"of {doc['superseded_by']!r}, which violates the NMDC schema."
                )
        dobj_superseded_by_map[doc_id] = superseded_by

    log.info(
        "Determining expectations for `superseded_by` fields of `DataObject`s, "
        "and generating `UpdateOne` statements that would achieve them."
    )
    for data_object_id, superseded_by in dobj_superseded_by_map.items():
        superseded_by_expected = SentinelValue.FIELD_ABSENT
        if data_object_id in wfe_expected_superseded_by_value_by_own_output_id.keys():
            superseded_by_expected = wfe_expected_superseded_by_value_by_own_output_id[
                data_object_id
            ]

        # If the document's `superseded_by` field already matches our expectation, proceed to
        # the next descriptor (rather than generating any "no op" `UpdateOne` statements).
        if superseded_by == superseded_by_expected:
            continue

        # Generate an `UpdateOne` statement that would make the `superseded_by` field
        # achieve our expectation of it (by either setting or unsetting the field).
        update = {"$set": {"superseded_by": superseded_by_expected}}
        if superseded_by_expected == SentinelValue.FIELD_ABSENT:
            update = {"$unset": {"superseded_by": True}}
        data_object_update = UpdateOne(filter={"id": data_object_id}, update=update)
        log.warning(f"Generated `UpdateOne`: {data_object_update!r}")
        data_object_set_update_statements.append(data_object_update)

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
                if len(update_statements) == 0:
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
