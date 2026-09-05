"""
Dagster ops related to managing the "superseded_by" field of documents in the MongoDB collections
named "workflow_execution_set" and "data_object_set".
"""

from dataclasses import dataclass

from dagster import DagsterLogManager, OpExecutionContext, op
from pymongo.database import Database

from nmdc_runtime.api.endpoints.lib.workflow_executions import parse_workflow_execution_id
from nmdc_runtime.api.models.query import UpdateCommand, UpdateStatement


@dataclass
class WorkflowExecutionDescriptor():
    """Encapsulates aspects of a `WorkflowExecution` that are related to the task at hand."""

    id: str
    """The `id` of the `WorkflowExecution` document."""

    run_number: int
    """The 'run number' derived from the `id` of the `WorkflowExecution` document."""

    has_output: list[str]
    """The `has_output` value of the `WorkflowExecution` document."""

    superseded_by: str | None | bool
    """The `superseded_by` value of the `WorkflowExecution` document. A value of `False` here is a sentinel value indicating that the document lacked this field."""

    superseded_by_expected: str | bool
    """The `superseded_by` value that correctly reflects the `WorkflowExecution` document's place in its supersession chain. A value of `False` here is a sentinel value indicating that the document should lack this field."""


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

    # Initialize a list of updates we'll perform on the "workflow_execution_set" collection.
    # Note: Each `updates` item will have a `q` field (the query) and a `u` field (the update).
    # Docs: https://www.mongodb.com/docs/manual/reference/command/update/
    workflow_execution_set_command = UpdateCommand(
        update="workflow_execution_set",
        updates=list(),
    )

    # Initialize a list of updates we'll perform on the "data_object_set" collection.
    data_object_set_command = UpdateCommand(
        update="data_object_set",
        updates=list(),
    )

    log.info(
        "Building LUT of all `WorkflowExecution` descriptors, "
        "grouped by base ID."
    )
    wfe_descriptors: dict[str, list[WorkflowExecutionDescriptor]] = dict()
    projection = dict(_id=False, id=True, has_output=True, superseded_by=True)
    cursor = workflow_execution_set.find(filter={}, projection=projection, batch_size=2_000)
    for doc in cursor:
        doc_id = doc["id"]
        base_id, run_number = parse_workflow_execution_id(doc_id)
        has_output: list[str] = doc["has_output"] if "has_output" in doc else list()
        # Note: Here (in the descriptor), we use `False` to represent the absence of the field (in the document).
        superseded_by: str | bool | None = doc["superseded_by"] if "superseded_by" in doc else False
        if base_id not in wfe_descriptors.keys():
            wfe_descriptors[base_id] = list()
        if run_number is None:
            raise ValueError(f"`WorkflowExecution` {doc_id!r} has no run number.")
        if any(run_number == wfe_desc.run_number for wfe_desc in wfe_descriptors[base_id]):
            raise ValueError(
                f"Multiple `WorkflowExecutions` have both base ID {base_id!r} "
                f"and run number {run_number!r}."
            )
        wfe_descriptor = WorkflowExecutionDescriptor(
            id=doc_id,
            run_number=run_number,
            has_output=has_output,
            superseded_by=superseded_by,
            superseded_by_expected=False,
        )
        wfe_descriptors[base_id].append(wfe_descriptor)

    log.info(
        "Sorting `WorkflowExecution` descriptors within each group, "
        "by run number."
    )
    for wfe_descriptors_for_base_id in wfe_descriptors.values():
        wfe_descriptors_for_base_id.sort(key=lambda d: d.run_number)

    log.info(
        "Checking for inadequate `superseded_by` fields among `WorkflowExecution`s, "
        "and generating `UpdateStatement`s."
    )
    for base_id, sorted_descriptors in wfe_descriptors.items():
        num_descriptors = len(sorted_descriptors)
        for idx, wfe_descriptor in enumerate(sorted_descriptors):
            # If this is the largest-run-numbered descriptor in the group, ensure its "superseded_by"
            # value is `False` (reminder: this is the descriptor, not the Mongo document).
            if idx + 1 == num_descriptors:
                wfe_descriptor.superseded_by_expected = False
                if wfe_descriptor.superseded_by is not False:
                    update_statement = UpdateStatement(
                        q={"id": wfe_descriptor.id},
                        u={"$unset": {"superseded_by": 1}},
                    )
                    log.debug(f"Generated `UpdateStatement`: {update_statement!r}")
                    workflow_execution_set_command.updates.append(update_statement)
            # Otherwise, this descriptor represents a `WorkflowExecution` that is superseded
            # by the next `WorkflowExecution` in the sequence. Ensure its "superseded_by" value
            # reflects that.
            else:
                wfe_descriptor.superseded_by_expected = sorted_descriptors[idx + 1].id
                if wfe_descriptor.superseded_by != wfe_descriptor.superseded_by_expected:
                    update_statement = UpdateStatement(
                        q={"id": wfe_descriptor.id},
                        u={"$set": {"superseded_by": wfe_descriptor.superseded_by_expected}},
                    )
                    log.debug(f"Generated `UpdateStatement`: {update_statement!r}")
                    workflow_execution_set_command.updates.append(update_statement)

    log.info(
        "Building LUT of the expected `superseded_by` value of each all `WorkflowExecution`, "
        "by distinct `has_output` value (i.e. `DataObject` `id`)."
    )
    wfe_expected_superseded_by_value_by_own_output_id: dict[str, str | None | bool] = dict()
    for sorted_descriptors in wfe_descriptors.values():
        for wfe_descriptor in sorted_descriptors:
            for data_object_id in wfe_descriptor.has_output:
                wfe_expected_superseded_by_value_by_own_output_id[data_object_id] = wfe_descriptor.superseded_by_expected

    log.info(
        "Building LUT mapping all `DataObject` `id` values to "
        "those `DataObject`s' current `superseded_by` values."
    )
    dobj_superseded_by_map: dict[str, str | bool | None] = dict()
    projection = dict(_id=False, id=True, superseded_by=True)
    cursor = data_object_set.find(filter={}, projection=projection, batch_size=2_000)
    for doc in cursor:
        doc_id = doc["id"]
        # Note: Here (in the map), we use `False` to represent the absence of the field (in the document).
        superseded_by: str | bool | None = doc["superseded_by"] if "superseded_by" in doc else False
        dobj_superseded_by_map[doc_id] = superseded_by

    log.info(
        "Checking for inadequate `superseded_by` fields among `DataObject`s, "
        "and generating `UpdateStatement`s."
    )
    for data_object_id, superseded_by in dobj_superseded_by_map.items():
        superseded_by_expected = False
        if data_object_id in wfe_expected_superseded_by_value_by_own_output_id:
            superseded_by_expected = wfe_expected_superseded_by_value_by_own_output_id[data_object_id]
        if superseded_by != superseded_by_expected:
            # Note: Even if the expected value is `None`, we go ahead and remove the field,
            #       since NMDC team members have established a convention of omitting
            #       "null"-valued fields from documents in schema-described collections.
            if superseded_by_expected in [False, None]:
                update_statement = UpdateStatement(
                    q={"id": data_object_id},
                    u={"$unset": {"superseded_by": True}},
                )
            else:
                update_statement = UpdateStatement(
                    q={"id": data_object_id},
                    u={"$set": {"superseded_by": superseded_by_expected}},
                )
            log.warning(f"Generated `UpdateStatement`: {update_statement!r}")
            data_object_set_command.updates.append(update_statement)

    log.info(
        "Number of `UpdateStatement`s generated for `workflow_execution_set`: "
        f"{len(workflow_execution_set_command.updates)}"
    )
    log.info(
        "Number of `UpdateStatement`s generated for `data_object_set`: "
        f"{len(data_object_set_command.updates)}"
    )

    # TODO: Apply the updates within a Mongo transaction (consider applying them in batches).
