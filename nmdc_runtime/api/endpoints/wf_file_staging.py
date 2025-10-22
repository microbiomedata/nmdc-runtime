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

from nmdc_runtime.api.models.wfe_file_stages import Globus, SequencingProject
from nmdc_runtime.api.models.user import User
from nmdc_runtime.api.endpoints.util import check_action_permitted

router = APIRouter()


def check_can_run_wf_file_staging_endpoints(user: User):
    """
    Check if the user is permitted to run the wf_file_staging endpoints in this file.
    """
    if not check_action_permitted(user.username, "/wf_file_staging:run"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to issue wf_file_staging commands.",
        )


@router.get(
    "/globus", response_model=ListResponse[Globus], response_model_exclude_unset=True
)
def list_globus_records(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    return list_resources(req, mdb, "globus")


@router.post(
    "/globus",
    status_code=status.HTTP_201_CREATED,
    response_model=Globus,
)
def create_globus_record(
    globus_in: Globus,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    globus_dict = globus_in.model_dump()
    mdb.globus.insert_one(globus_dict)
    return globus_dict


@router.get("/globus/{task_id}", response_model=Globus)
def get_globus(
    task_id: str,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    print(f"Getting Globus record for task_id: {task_id}")
    check_can_run_wf_file_staging_endpoints(user)
    print("Permission check passed.")
    return raise404_if_none(mdb.globus.find_one({"task_id": task_id}))


@router.patch("/globus/{task_id}", response_model=Globus)
def update_globus(
    task_id: str,
    globus_patch: Globus,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    check_can_run_wf_file_staging_endpoints(user)

    doc = raise404_if_none(mdb.globus.find_one({"task_id": task_id}))
    doc_globus_patched = merge(doc, globus_patch.model_dump(exclude_unset=True))
    mdb.globus.replace_one({"task_id": task_id}, doc_globus_patched)
    return doc_globus_patched



@router.get(
    "/sequencing-project", response_model=ListResponse[SequencingProject], response_model_exclude_unset=True
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
    "/sequencing-project",
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


@router.get("/sequencing-project/{project_name}", response_model=SequencingProject)
def get_sequencing_project(
    project_name: str,
    mdb: Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    # check for permissions first
    print(f"Getting Globus record for task_id: {project_name}")
    check_can_run_wf_file_staging_endpoints(user)
    print("Permission check passed.")
    return raise404_if_none(mdb.sequencingproject.find_one({"project_name": project_name}))


