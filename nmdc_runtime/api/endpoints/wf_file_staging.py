from fastapi import APIRouter, Depends
from pymongo.database import Database
from typing import Annotated
from fastapi import APIRouter, Depends, Query
from toolz import merge

from nmdc_runtime.api.core.util import raise404_if_none, HTTPException, status
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.util import ListRequest, ListResponse
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.site import (
    Site,
    get_current_client_site,
)
from nmdc_runtime.api.models.wfe_file_stages import Globus
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
    mdb: Database = Depends(get_mongo_db)
):
    return list_resources(req, mdb, "globus")


@router.get("/globus/{task_id}", response_model=ListResponse[Globus])
def get_globus(
    task_id: str,
    mdb: Database = Depends(get_mongo_db),
):
    op = raise404_if_none(mdb.globus.find_one({"task_id": task_id}))
    return op

@router.patch("/globus/{task_id}", response_model=Globus)
def update_object(
    object_id: str,
    object_patch: Globus,
    mdb: Database = Depends(get_mongo_db),
):
    doc = raise404_if_none(mdb.globus.find_one({"id": object_id}))
    doc_globus_patched = merge(doc, object_patch.model_dump(exclude_unset=True))
    mdb.globus.replace_one({"id": object_id}, doc_globus_patched)
    return doc_globus_patched