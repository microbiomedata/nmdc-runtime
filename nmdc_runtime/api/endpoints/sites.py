from datetime import timedelta, datetime, timezone
from typing import List
from uuid import uuid4

import botocore
from fastapi import APIRouter, Response, Depends, status, HTTPException
import pymongo.database
from starlette.status import HTTP_403_FORBIDDEN

from nmdc_runtime.api.core.auth import (
    TokenExpires,
    Token,
    create_access_token,
    get_access_token_expiration,
    ClientCredentials,
    get_password_hash,
)
from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.db.s3 import get_s3_client, presigned_url_to_put
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object import DrsObjectBlobIn, Error
from nmdc_runtime.api.models.site import get_current_client_site, Site
from nmdc_runtime.api.models.user import (
    get_current_active_user,
    User,
)

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
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    s3client: botocore.client.BaseClient = Depends(get_s3_client),
    site: Site = Depends(get_current_client_site),
):
    client_site_id = site.id
    site = raise404_if_none(
        mdb.sites.find_one({"id": site_id}), detail=f"no site with ID '{site_id}'"
    )
    if site["id"] != client_site_id:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=f"client authorized for different site_id than {site_id}",
        )
    expires_in = 300
    id_ns = "do"  # Drs Objects.
    eid = generate_id_unique(mdb, id_ns)
    url = presigned_url_to_put(
        f"{id_ns}/{eid}",
        client=s3client,
        mime_type=object_in.mime_type,
        expires_in=expires_in,
    )
    # TODO return Operation that site client can update
    #   to signal runtime to create object resource.
    return {"url": url, "expires_in": expires_in}


@router.post("/sites/{site_id}:generateCredentials", response_model=ClientCredentials)
def generate_credentials_for_site_client(
    site_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    raise404_if_none(
        mdb.sites.find_one({"id": site_id}), detail=f"no site with ID '{site_id}'"
    )
    site_admin = mdb.users.find_one({"username": user.username, "site_admin": site_id})
    if not site_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You're not registered as an admin for this site",
        )

    client_id = generate_id_unique(mdb, "site_clients")
    client_secret = uuid4().hex
    hashed_secret = get_password_hash(client_secret)
    mdb.sites.update_one(
        {"id": site_id},
        {"$push": {"clients": {"id": client_id, "hashed_secret": hashed_secret}}},
    )

    return {
        "client_id": client_id,
        "client_secret": client_secret,
    }
