from typing import Optional

from pymongo.client_session import ClientSession
from pymongo.database import Database


def remove_from_supersession_chain(
    workflow_execution: dict,
    db: Database,
    client_session: Optional[ClientSession] = None,
) -> None:
    """
    Removes the specified `WorkflowExecution` from any `superseded_by` chain(s) it belongs to,
    updating the `superseded_by` fields of upstream `WorkflowExecution`s and `DataObject`s
    accordingly.

    Examples (where A/B/C represent `WorkflowExecution`s and `→` represents a `superseded_by` field):
    - If "A → B → C" and we remove "A", the result will be "B → C".
    - If "A → B → C" and we remove "B", the result will be "A → C".
    - If "A → B → C" and we remove "C", the result will be "A → B".

    :param workflow_execution: Dictionary representing the `WorkflowExecution` you want to remove
                               from any supersession chains it belongs to.
    :param db: Database instance to use for updating `superseded_by` fields.
    :param client_session: Optional `ClientSession`, which the function will use to perform the
                           database operations within the existing MongoDB transaction, if any.

    Reference: https://microbiomedata.github.io/nmdc-schema/superseded_by/
    """

    # Get the `id` of the `WorkflowExecution`, if any, that supersedes the subject one.
    subject_workflow_execution_id = workflow_execution["id"]
    wfe_superseding_subject_workflow_execution = (
        workflow_execution["superseded_by"]
        if "superseded_by" in workflow_execution
        else None
    )

    # Get references to relevant MongoDB collections.
    workflow_execution_set = db.get_collection("workflow_execution_set")
    data_object_set = db.get_collection("data_object_set")

    # Update the `superseded_by` fields of all upstream (superseded) `WorkflowExecution`s (WFEs)
    # and `DataObject`s (DOs), if any, to reflect the removal of the subject WFE from the chain.
    #
    # Scenario 1: If the subject WFE is _not_ `superseded_by` any WFE or DO, we will just delete the
    #             `superseded_by` fields from all WFEs/DOs that are superseded by the subject one.
    #             i.e., if "WFE1 → WFE2" and subject is "WFE2", delete the field from "WFE1".
    #
    # Scenario 2: If the subject WFE _is_ `superseded_by` any WFE or DO, we will update the
    #             `superseded_by` fields of all WFEs/DOs that are `superseded_by` the subject one,
    #             so those fields contain the `id` of the WFE that supersedes the subject one.
    #             i.e., if "WFE1 → WFE2 → WFE3" and subject is "WFE2", update the field on "WFE1".
    #
    if wfe_superseding_subject_workflow_execution is None:
        operation = {"$unset": {"superseded_by": ""}}
    else:
        operation = {
            "$set": {"superseded_by": wfe_superseding_subject_workflow_execution}
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

    # Remove the `superseded_by` field, if any, from the subject `WorkflowExecution`,
    # effectively removing that `WorkflowExecution` from the supersession chain, if any.
    workflow_execution_set.update_one(
        {"id": subject_workflow_execution_id},
        {"$unset": {"superseded_by": ""}},
        session=client_session,
    )
