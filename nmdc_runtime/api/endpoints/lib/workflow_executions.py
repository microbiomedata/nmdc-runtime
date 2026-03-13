from typing import Optional

from pymongo.client_session import ClientSession
from pymongo.database import Database


def prepare_supersession_chain_for_workflow_execution_deletion(
    workflow_execution: dict,
    db: Database,
    client_session: Optional[ClientSession] = None,
) -> None:
    """
    Updates or deletes the `superseded_by` field of each `WorkflowExecution` and `DataObject`
    whose `superseded_by` field currently points to the subject `WorkflowExecution` (i.e.
    the one you are preparing to delete), so that that field no longer points to it.

    There are two possible scenarios:
    1. If _no_ `WorkflowExecution` supersedes the subject `WorkflowExecution`, this function will
       delete the `superseded_by` field of each `WorkflowExecution` and `DataObject` that is
       superseded by the subject one; for example:
       If "WFE1 → WFE2" and "DO1 → WFE2" and the subject is "WFE2", this
       function will delete the `superseded_by` field from "WFE1" and from "DO1".
    2. If a `WorkflowExecution` _does_ supersede the subject `WorkflowExecution`, this function will
       update the `superseded_by` field of each `WorkflowExecution` and `DataObject` that is
       superseded by the subject one, so that that field contains the `id` of
       the `WorkflowExecution` that _supersedes_ the subject one; for example:
       If "WFE1 → WFE2 → WFE3" and "DO1 → WFE2 → WFE3" and the subject is "WFE2", this
       function will update the `superseded_by` field of "WFE1" and of "DO1" to point to "WFE3".

    :param workflow_execution: Dictionary representing the `WorkflowExecution` for whose deletion
                               you want to prepare the supersession chain. We call this the
                               "subject" `WorkflowExecution`.
    :param db: Database instance to use for updating `superseded_by` fields.
    :param client_session: Optional `ClientSession`, which the function will use to perform the
                           database operations within the existing MongoDB transaction, if any.

    Reference: https://microbiomedata.github.io/nmdc-schema/superseded_by/
    """

    # Get the `id` of the `WorkflowExecution`, if any, that supersedes the subject one;
    # i.e., if "WFE1 → WFE2" and the subject is "WFE1", get the `id` of "WFE2".
    subject_workflow_execution_id = workflow_execution["id"]
    id_of_wfe_superseding_subject_workflow_execution = (
        workflow_execution["superseded_by"]
        if "superseded_by" in workflow_execution
        else None
    )

    # Get references to relevant MongoDB collections.
    workflow_execution_set = db.get_collection("workflow_execution_set")
    data_object_set = db.get_collection("data_object_set")

    # Update or delete the `superseded_by` field of each `WorkflowExecution` and `DataObject`
    # that is currently superseded by the subject `WorkflowExecution`, so that that field
    # no longer points to that `WorkflowExecution`.
    if id_of_wfe_superseding_subject_workflow_execution is None:
        operation = {"$unset": {"superseded_by": ""}}
    else:
        operation = {
            "$set": {"superseded_by": id_of_wfe_superseding_subject_workflow_execution}
        }
    workflow_execution_set.update_many(
        {"superseded_by": subject_workflow_execution_id},
        operation,
        session=client_session,
    )
    data_object_set.update_many(
        {"superseded_by": subject_workflow_execution_id},
        operation,
        session=client_session,
    )
