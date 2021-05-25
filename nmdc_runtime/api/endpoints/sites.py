from typing import List

import botocore
from fastapi import APIRouter, Response, Depends, status
import pymongo

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.db.s3 import get_s3_client, presigned_url_to_put
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object import DrsObjectBlobIn, Error
from nmdc_runtime.api.models.user import get_current_active_user, User

router = APIRouter()


@router.post("/sites")
def create_site():
    pass


@router.get("/sites")
def list_sites():
    pass


@router.get("/sites/{site_id}")
def get_site():
    pass


@router.patch("/sites/{site_id}")
def update_site():
    pass


@router.put("/sites/{site_id}")
def replace_site():
    pass


@router.get("/sites/{site_id}/capabilities", response_model=List[Capability])
def list_site_capabilities(site_id: str):
    return site_id


@router.put("/sites/{site_id}/capabilities", response_model=List[Capability])
def replace_site_capabilities(site_id: str, capability_ids: List[str]):
    return capability_ids


@router.post("/sites/{site_id}:putObject")
def put_object_in_site(
    site_id: str,
    object_in: DrsObjectBlobIn,
    response: Response,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    s3client: botocore.client.BaseClient = Depends(get_s3_client),
    user: User = Depends(get_current_active_user),
):
    site = mdb.sites.find_one({"id": site_id})
    if site is None:
        response.status_code = 404
        return {"msg": f"no site with ID '{site_id}'"}
    expires_in = 300
    id_ns = "do"  # Drs Objects.
    eid = generate_id_unique(mdb, id_ns)
    url = presigned_url_to_put(
        f"{id_ns}/{eid}",
        client=s3client,
        mime_type=object_in.mime_type,
        expires_in=expires_in,
    )
    # TODO return Operation that user can update to signal runtime to create object resource.
    return {"url": url, "expires_in": expires_in}
