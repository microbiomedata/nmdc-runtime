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
    updating the `superseded_by` fields of upstream `WorkflowExecution`s accordingly.

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
    wfe_id = workflow_execution["id"]
    wfe_superseding_subject_wfe = workflow_execution["superseded_by"] if "superseded_by" in workflow_execution else None

    # Remove the `superseded_by` field, if any, from the subject `WorkflowExecution`,
    # effectively removing it from the chain, if any.
    wfe_set = db.get_collection("workflow_execution_set")
    wfe_set.update_one({"id": wfe_id}, {"$unset": {"superseded_by": ""}}, session=client_session)

    # Update the `superseded_by` fields to reflect the removal of the subject WFE from the chain.
    #
    # If the subject WFE is not superseded by any other WFE, we will just delete the `superseded_by`
    # field from all WFEs that are superseded by the subject one.
    #
    # If the subject WFE is superseded by another WFE, we will update the `superseded_by` field of
    # all WFEs that are superseded by the subject one, so they contain the `id` of the WFE that
    # supersedes the subject one.
    #
    if wfe_superseding_subject_wfe is None:
        operation = {"$unset": {"superseded_by": ""}}
    else:
        operation = {"$set": {"superseded_by": wfe_superseding_subject_wfe}}
    wfe_set.update_many({"superseded_by": wfe_id}, operation, session=client_session)
