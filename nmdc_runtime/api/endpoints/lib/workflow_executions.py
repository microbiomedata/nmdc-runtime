import re
from typing import List, Optional, Tuple

from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.collection import Collection


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


def parse_workflow_execution_id(raw_id: str) -> Tuple[str, int | None]:
    """
    Parse a `WorkflowExecution` ID into its base ID and run number components.

    The NMDC workflow automation team members have established a convention for indicating
    whether a workflow execution is a "re-run" or not.

    When they populate the `id` field of a `WorkflowExecution, they append a ".{integer > 0}"
    suffix to it (e.g. "nmdc:wfmp-00-abcdef" → "nmdc:wfmp-00-abcdef.1").

    Examples:
    - When the integer == 1, the `WorkflowExecution` is a first run (i.e. not a re-run).
    - When the integer == 2, the `WorkflowExecution` is a second run (i.e. it is a re-run
      and supersedes the `WorkflowExecution` whose `id` has the integer == 1), etc.

    :param raw_id: The raw `WorkflowExecution` ID string to parse

    :returns: A tuple containing the parsed result. The tuple has the following items:
              - "base_id": the base ID portion of the ID, if the ID ends with ".{integer > 0}";
                           otherwise, the entire ID.
              - "run_number": the integer suffix of the ID, if the ID ends with ".{integer > 0}";
                              otherwise, `None`.

    >>> parse_workflow_execution_id("nmdc:wfmp-00-abcdef")  # does not match pattern
    ('nmdc:wfmp-00-abcdef', None)
    >>> parse_workflow_execution_id("nmdc:wfmp-00-abcdef.")  # does not match pattern
    ('nmdc:wfmp-00-abcdef.', None)
    >>> parse_workflow_execution_id("nmdc:wfmp-00-abcdef.0")  # integer is too low
    ('nmdc:wfmp-00-abcdef.0', None)

    >>> parse_workflow_execution_id("nmdc:wfmp-00-abcdef.1")  # lowest integer (also single-digit)
    ('nmdc:wfmp-00-abcdef', 1)
    >>> parse_workflow_execution_id("nmdc:wfmp-00-abcdef.123")  # multiple digits
    ('nmdc:wfmp-00-abcdef', 123)
    """

    result = (raw_id, None)  # fallback value

    # Define a regular expression pattern that matches a string having a ".{integer}" suffix,
    # and captures the substrings before (in group 1) and after (in group 2) the final period.
    workflow_execution_id_pattern = re.compile(r"^(.+)\.(\d+)$")

    match = re.search(workflow_execution_id_pattern, raw_id)
    if match is not None and len(match.groups()) == 2:
        base_id = match.group(1)
        run_number = int(match.group(2))
        if run_number >= 1:
            result = (base_id, run_number)

    return result


def derive_predecessor_id(workflow_execution_id: str) -> Optional[str]:
    """
    Derive the `id` of the hypothetical `WorkflowExecution` that would be superseded by
    the `WorkflowExecution` having the specified `id`.

    Reference: https://microbiomedata.github.io/nmdc-schema/WorkflowExecution/

    >>> derive_predecessor_id("nmdc:wfmp-00-abcdef") is None  # does not match pattern
    True
    >>> derive_predecessor_id("nmdc:wfmp-00-abcdef.1") is None  # already lowest integer
    True

    >>> derive_predecessor_id("nmdc:wfmp-00-abcdef.2")
    'nmdc:wfmp-00-abcdef.1'
    >>> derive_predecessor_id("nmdc:wfmp-00-abcdef.123")
    'nmdc:wfmp-00-abcdef.122'
    """

    predecessor_id = None

    base_id, run_number = parse_workflow_execution_id(workflow_execution_id)
    if run_number is not None and run_number > 0:
        predecessor_run_number = run_number - 1
        if predecessor_run_number >= 1:  # enforces minimum run number of 1
            predecessor_id = f"{base_id}.{predecessor_run_number}"

    return predecessor_id


def derive_successor_id(workflow_execution_id: str) -> Optional[str]:
    """
    Derive the `id` of the hypothetical `WorkflowExecution` that would supersede
    the `WorkflowExecution` having the specified `id`.

    Reference: https://microbiomedata.github.io/nmdc-schema/WorkflowExecution/

    >>> derive_successor_id("nmdc:wfmp-00-abcdef") is None  # does not match pattern
    True

    >>> derive_successor_id("nmdc:wfmp-00-abcdef.1")
    'nmdc:wfmp-00-abcdef.2'
    >>> derive_successor_id("nmdc:wfmp-00-abcdef.123")
    'nmdc:wfmp-00-abcdef.124'
    """

    successor_id = None

    base_id, run_number = parse_workflow_execution_id(workflow_execution_id)
    if run_number is not None:
        successor_run_number = run_number + 1
        if successor_run_number >= 1:
            successor_id = f"{base_id}.{successor_run_number}"

    return successor_id


def get_predecessor_from_mongo_collection_by_own_id(
    workflow_execution_id: str, collection: Collection
) -> Optional[dict]:
    """
    Get the `WorkflowExecution` document, if any, whose `id` indicates that it is the predecessor
    of the specified `WorkflowExecution`; otherwise, return `None`.
    """

    predecessor_document = None

    predecessor_id = derive_predecessor_id(workflow_execution_id)
    if predecessor_id is not None:
        predecessor_document = collection.find_one({"id": predecessor_id})

    return predecessor_document


def get_successor_from_mongo_collection_by_own_id(
    workflow_execution_id: str, collection: Collection
) -> Optional[dict]:
    """
    Get the `WorkflowExecution` document, if any, whose `id` indicates that it is the successor
    to the specified `WorkflowExecution`; otherwise, return `None`.
    """

    successor_document = None

    successor_id = derive_successor_id(workflow_execution_id)
    if successor_id is not None:
        successor_document = collection.find_one({"id": successor_id})

    return successor_document


def get_predecessor_from_list_by_own_id(
    workflow_execution_id: str, workflow_execution_set: List[dict]
) -> Optional[dict]:
    """
    Get the `WorkflowExecution` document, if any—from the specified list of `WorkflowExecution`
    documents—whose `id` indicates that it is the predecessor of the specified `WorkflowExecution`;
    otherwise, return `None`.

    >>> workflow_execution_set = [
    ...     {"id": "nmdc:wfmp-00-abcdef.1"},
    ...     {"id": "nmdc:wfmp-00-abcdef.2"},
    ...     {"id": "nmdc:wfmp-00-abcdef.3"},
    ... ]
    >>> get_predecessor_from_list_by_own_id("nmdc:wfmp-00-abcdef.2", workflow_execution_set)
    {'id': 'nmdc:wfmp-00-abcdef.1'}
    >>> get_predecessor_from_list_by_own_id("nmdc:wfmp-00-abcdef.1", workflow_execution_set) is None
    True
    """

    predecessor_document = None

    predecessor_id = derive_predecessor_id(workflow_execution_id)
    if predecessor_id is not None:
        for wfe in workflow_execution_set:
            if wfe["id"] == predecessor_id:
                predecessor_document = wfe
                break

    return predecessor_document


def get_successor_from_list_by_own_id(
    workflow_execution_id: str, workflow_execution_set: List[dict]
) -> Optional[dict]:
    """
    Get the `WorkflowExecution` document, if any—from the specified list of `WorkflowExecution`
    documents—whose `id` indicates that it is the successor to the specified `WorkflowExecution`;
    otherwise, return `None`.

    >>> workflow_execution_set = [
    ...     {"id": "nmdc:wfmp-00-abcdef.1"},
    ...     {"id": "nmdc:wfmp-00-abcdef.2"},
    ...     {"id": "nmdc:wfmp-00-abcdef.3"},
    ... ]
    >>> get_successor_from_list_by_own_id("nmdc:wfmp-00-abcdef.2", workflow_execution_set)
    {'id': 'nmdc:wfmp-00-abcdef.3'}
    >>> get_successor_from_list_by_own_id("nmdc:wfmp-00-abcdef.3", workflow_execution_set) is None
    True
    """

    successor_document = None

    successor_id = derive_successor_id(workflow_execution_id)
    if successor_id is not None:
        for wfe in workflow_execution_set:
            if wfe["id"] == successor_id:
                successor_document = wfe
                break

    return successor_document


def make_pattern_matching_ids_having_base_id(base_id: str) -> str:
    """
    Make a regular expression pattern that matches `WorkflowExecution` `id` values that share the
    specified base ID.

    >>> make_pattern_matching_ids_having_base_id("foo")
    '^foo\\\\.\\\\d+$'
    >>> make_pattern_matching_ids_having_base_id("nmdc:wfmp-00-abcdef")
    '^nmdc:wfmp\\\\-00\\\\-abcdef\\\\.\\\\d+$'
    """

    escaped_base_id = re.escape(base_id)
    regex_pattern = (
        f"^{escaped_base_id}\\.\\d+$"  # double escape, since f-string (not r-string)
    )
    return regex_pattern
