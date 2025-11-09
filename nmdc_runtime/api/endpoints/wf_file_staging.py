from fastapi import APIRouter, Depends, Query
from pymongo.database import Database
from typing import Annotated
from toolz import merge
import logging

from nmdc_runtime.api.core.util import raise404_if_none, HTTPException, status
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import (
    check_action_permitted,
    list_resources,
    strip_oid,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.util import ListRequest, ListResponse
from nmdc_runtime.api.models.wfe_file_stages import (
    GlobusTask,
    GlobusTaskStatus,
    JDPFileStatus,
    JGISample,
    JGISequencingProject,
    WorkflowFileStagingCollectionName as CollectionName,
)

router = APIRouter()


def check_can_run_wf_file_staging_endpoints(user: User):
    """
    Check if the user is permitted to run the wf_file_staging endpoints in this file.
    """
    if not check_action_permitted(user.username, "/wf_file_staging"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to issue wf_file_staging commands.",
        )


@router.post(
    "/wf_file_staging/globus_tasks",
    status_code=status.HTTP_201_CREATED,
    response_model=GlobusTask,
)
def create_globus_tasks(
    globus_in: GlobusTask,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Create a `GlobusTask`."""

    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)
    # check if record with same task_id already exists
    existing = mdb["wf_file_staging.globus_tasks"].find_one(
        {"task_id": globus_in.task_id}
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Globus task with task_id {globus_in.task_id} already exists.",
        )
    # check the status exists in the Enum, if not log a warning
    if globus_in.task_status not in GlobusTaskStatus.__members__.values():
        logging.warning(
            f"Globus task status {globus_in.task_status} does not exist in GlobusTaskStatus enum."
        )

    globus_dict = globus_in.model_dump()
    mdb["wf_file_staging.globus_tasks"].insert_one(globus_dict)
    return globus_dict


@router.get("/wf_file_staging/globus_tasks/{task_id}", response_model=GlobusTask)
def get_globus_tasks(
    task_id: str,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Retrieve a `GlobusTask`."""

    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)
    return raise404_if_none(
        mdb["wf_file_staging.globus_tasks"].find_one({"task_id": task_id})
    )


@router.patch("/wf_file_staging/globus_tasks/{task_id}", response_model=GlobusTask)
def update_globus_tasks(
    task_id: str,
    globus_patch: GlobusTask,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Update a `GlobusTask`."""

    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    if task_id != globus_patch.task_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_id in path and body must match.",
        )

    doc = raise404_if_none(
        mdb["wf_file_staging.globus_tasks"].find_one({"task_id": task_id})
    )
    doc_globus_patched = merge(doc, globus_patch.model_dump(exclude_unset=True))
    mdb["wf_file_staging.globus_tasks"].replace_one(
        {"task_id": task_id}, doc_globus_patched
    )
    return doc_globus_patched


# Note: We use the generic `Doc` class—instead of the `GlobusTask` class—to describe the response
#       because this endpoint (via `ListRequest`) supports projection, which can be used to omit
#       fields from the response, even fields the `GlobusTask` class says are required.
@router.get(
    "/wf_file_staging/globus_tasks",
    response_model=ListResponse[Doc],
    response_model_exclude_unset=True,
)
def list_globus_tasks(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Get a list of `GlobusTask`s."""

    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)
    rv = list_resources(req, mdb, "wf_file_staging.globus_tasks")
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


@router.post(
    "/wf_file_staging/jgi_samples",
    status_code=status.HTTP_201_CREATED,
    response_model=JGISample,
)
def create_jgi_sample(
    jgi_in: JGISample,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """
    Create a JGI Sample.
    """

    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)
    # check if record with same jdp_file_id already exists
    existing = mdb["wf_file_staging.jgi_samples"].find_one(
        {"jdp_file_id": jgi_in.jdp_file_id}
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"JGI sample with jdp_file_id {jgi_in.jdp_file_id} already exists.",
        )
    # check the status exists in the enum, if not log a warning
    if jgi_in.jdp_file_status not in JDPFileStatus.__members__.values():
        logging.warning(
            f"JDP file status {jgi_in.jdp_file_status} does not exist in JDPFileStatus enum."
        )
    if jgi_in.globus_file_status not in GlobusTaskStatus.__members__.values():
        logging.warning(
            f"Globus file status {jgi_in.globus_file_status} does not exist in GlobusTaskStatus enum."
        )

    sample_dict = jgi_in.model_dump(exclude_unset=True)
    try:
        mdb["wf_file_staging.jgi_samples"].insert_one(sample_dict)
        return sample_dict
    except Exception as e:
        logging.error(f"Error during jgi sample insertion: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during insertion: {str(e)}",
        )


# Note: We use the generic `Doc` class—instead of the `JGISample` class—to describe the response
#       because this endpoint (via `ListRequest`) supports projection, which can be used to omit
#       fields from the response, even fields the `JGISample` class says are required.
@router.get(
    "/wf_file_staging/jgi_samples",
    response_model=ListResponse[Doc],
    response_model_exclude_unset=True,
)
def list_jgi_samples(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    r"""
    Retrieves JGI Sample records that match the specified filter criteria. Uses Mongo-like filters.
    """
    # perm check
    check_can_run_wf_file_staging_endpoints(user)

    rv = list_resources(req, mdb, "wf_file_staging.jgi_samples")
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


@router.patch("/wf_file_staging/jgi_samples/{jdp_file_id}", response_model=JGISample)
def update_jgi_samples(
    jdp_file_id: str,
    jgi_sample_patch: JGISample,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """
    Update a JGI Sample record by its jdp_file_id.
    """
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    if jdp_file_id != jgi_sample_patch.jdp_file_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot modify jdp_file_id (jdp_file_id in path and body must match).",
        )

    doc_jgi_sample_original = raise404_if_none(
        mdb["wf_file_staging.jgi_samples"].find_one({"jdp_file_id": jdp_file_id})
    )
    doc_jgi_sample_patched = merge(
        doc_jgi_sample_original, jgi_sample_patch.model_dump(exclude_unset=True)
    )
    mdb["wf_file_staging.jgi_samples"].replace_one(
        {"jdp_file_id": jdp_file_id}, doc_jgi_sample_patched
    )
    return doc_jgi_sample_patched


# Note: We use the generic `Doc` class—instead of the `JGISequencingProject` class—to describe the response
#       because this endpoint (via `ListRequest`) supports projection, which can be used to omit
#       fields from the response, even fields the `JGISequencingProject` class says are required.
@router.get(
    "/wf_file_staging/jgi_sequencing_projects",
    response_model=ListResponse[Doc],
    response_model_exclude_unset=True,
)
def list_sequencing_project_records(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Get a list of `JGISequencingProject`s."""

    check_can_run_wf_file_staging_endpoints(user)
    rv = list_resources(req, mdb, CollectionName.JGI_SEQUENCING_PROJECTS.value)
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


@router.post(
    "/wf_file_staging/jgi_sequencing_projects",
    status_code=status.HTTP_201_CREATED,
    response_model=JGISequencingProject,
)
def create_sequencing_record(
    sequencing_project_in: JGISequencingProject,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Create a `JGISequencingProject`."""

    check_can_run_wf_file_staging_endpoints(user)
    existing = mdb[CollectionName.JGI_SEQUENCING_PROJECTS.value].find_one(
        {"sequencing_project_name": sequencing_project_in.sequencing_project_name}
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"JGISequencingProject with project name {sequencing_project_in.sequencing_project_name} already exists.",
        )
    sequencing_project_dict = sequencing_project_in.model_dump()
    mdb[CollectionName.JGI_SEQUENCING_PROJECTS.value].insert_one(
        sequencing_project_dict
    )
    return sequencing_project_dict


@router.get(
    "/wf_file_staging/jgi_sequencing_projects/{sequencing_project_name}",
    response_model=JGISequencingProject,
)
def get_sequencing_project(
    sequencing_project_name: str,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """Retrieve a `JGISequencingProject`."""

    check_can_run_wf_file_staging_endpoints(user)

    return raise404_if_none(
        mdb[CollectionName.JGI_SEQUENCING_PROJECTS.value].find_one(
            {"sequencing_project_name": sequencing_project_name}
        )
    )
