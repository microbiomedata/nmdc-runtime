from fastapi import APIRouter, Depends
from pymongo.database import Database
from typing import Annotated
from fastapi import APIRouter, Depends, Query
from toolz import merge

from nmdc_runtime.api.core.util import raise404_if_none, HTTPException, status
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.util import ListRequest, ListResponse
from nmdc_runtime.api.endpoints.util import list_resources

from nmdc_runtime.api.models.wfe_file_stages import GlobusTask, GlobusTaskStatus, SequencingProject
from nmdc_runtime.api.models.user import User
from nmdc_runtime.api.endpoints.util import check_action_permitted

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


@router.get(
    "/wf_file_staging/globus_tasks",
    response_model=ListResponse[GlobusTask],
    response_model_exclude_unset=True,
)
def list_globus_tasks(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    return list_resources(req, mdb, "wf_file_staging.globus_tasks")


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
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)
    # check if record with same task_id already exists
    existing = mdb.wf_file_staging.globus_tasks.find_one({"task_id": globus_in.task_id})
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Globus task with task_id {globus_in.task_id} already exists.",
        )
    # check the status exists in the Enum, if not log a warning
    if globus_in.task_status not in GlobusTaskStatus.__members__.values():
        print(
            f"Warning: Globus task status {globus_in.task_status} does not exist in GlobusTaskStatus enum."
        )

    globus_dict = globus_in.model_dump()
    mdb.wf_file_staging.globus_tasks.insert_one(globus_dict)
    return globus_dict


@router.get("/wf_file_staging/globus_tasks/{task_id}", response_model=GlobusTask)
def get_globus_tasks(
    task_id: str,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)
    return raise404_if_none(
        mdb.wf_file_staging.globus_tasks.find_one({"task_id": task_id})
    )


@router.patch("/wf_file_staging/globus_tasks/{task_id}", response_model=GlobusTask)
def update_globus_tasks(
    task_id: str,
    globus_patch: GlobusTask,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    if task_id != globus_patch.task_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_id in path and body must match.",
        )

    doc = raise404_if_none(
        mdb.wf_file_staging.globus_tasks.find_one({"task_id": task_id})
    )
    doc_globus_patched = merge(doc, globus_patch.model_dump(exclude_unset=True))
    mdb.wf_file_staging.globus_tasks.replace_one(
        {"task_id": task_id}, doc_globus_patched
    )
    return doc_globus_patched



@router.get(
    "/wf_file_staging/sequencing-project", response_model=ListResponse[SequencingProject], response_model_exclude_unset=True
)
def list_sequencing_project_records(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    return list_resources(req, mdb, "sequencing_project")


@router.post(
    "/wf_file_staging/sequencing-project",
    status_code=status.HTTP_201_CREATED,
    response_model=SequencingProject,
)
def create_sequencing_record(
    sequencing_project_in: SequencingProject,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    sequencing_project_dict = sequencing_project_in.model_dump()
    mdb.sequencing_project.insert_one(sequencing_project_dict)
    return sequencing_project_dict


@router.get("/wf_file_staging/sequencing-project/{project_name}", response_model=SequencingProject)
def get_sequencing_project(
    project_name: str,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    print(f"Getting SequencingProject record for project_name: {project_name}")
    check_can_run_wf_file_staging_endpoints(user)
    print("Permission check passed.")
    return raise404_if_none(mdb.sequencingproject.find_one({"project_name": project_name}))


