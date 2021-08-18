from typing import List

import botocore
import pymongo.database
from fastapi import APIRouter, Depends, status, HTTPException
from starlette.status import HTTP_403_FORBIDDEN

from nmdc_runtime.api.core.auth import (
    ClientCredentials,
    get_password_hash,
)
from nmdc_runtime.api.core.idgen import generate_one_id, local_part
from nmdc_runtime.api.core.util import (
    raise404_if_none,
    expiry_dt_from_now,
    dotted_path_for,
    generate_secret,
    API_SITE_ID,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.db.s3 import (
    get_s3_client,
    presigned_url_to_put,
    presigned_url_to_get,
    S3_ID_NS,
)
from nmdc_runtime.api.endpoints.util import exists, list_resources
from nmdc_runtime.api.models.capability import Capability
from nmdc_runtime.api.models.object import (
    AccessMethod,
    AccessURL,
    DrsObjectBase,
    DrsObjectIn,
)
from nmdc_runtime.api.models.operation import Operation, ObjectPutMetadata
from nmdc_runtime.api.models.site import (
    get_current_client_site,
    Site,
    SiteInDB,
)
from nmdc_runtime.api.models.user import get_current_active_user, User
from nmdc_runtime.api.models.util import ListResponse, ListRequest

router = APIRouter()


@router.post("/sites", status_code=status.HTTP_201_CREATED, response_model=SiteInDB)
def create_site(
    site: Site,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    if exists(mdb.sites, {"id": site.id}):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"site with supplied id {site.id} already exists",
        )
    mdb.sites.insert_one(site.dict())
    rv = mdb.users.update_one(
        {"username": user.username},
        {"$addToSet": {"site_admin": site.id}},
    )
    if rv.modified_count != 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to register user {user.username} as site_admin for site {site.id}.",
        )
    return mdb.sites.find_one({"id": site.id})


@router.get(
    "/sites", response_model=ListResponse[Site], response_model_exclude_unset=True
)
def list_sites(
    req: ListRequest = Depends(),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list_resources(req, mdb, "sites")


@router.get("/sites/{site_id}", response_model=Site, response_model_exclude_unset=True)
def get_site(
    site_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.sites.find_one({"id": site_id}))


@router.patch("/sites/{site_id}", include_in_schema=False)
def update_site():
    """Not yet implemented"""
    pass


@router.put("/sites/{site_id}", include_in_schema=False)
def replace_site():
    """Not yet implemented"""
    pass


@router.get("/sites/{site_id}/capabilities", response_model=List[Capability])
def list_site_capabilities(site_id: str):
    return site_id


@router.put("/sites/{site_id}/capabilities", response_model=List[Capability])
def replace_site_capabilities(site_id: str, capability_ids: List[str]):
    return capability_ids


def verify_client_site_pair(
    site_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    client_site: Site = Depends(get_current_client_site),
):
    site = raise404_if_none(
        mdb.sites.find_one({"id": site_id}), detail=f"no site with ID '{site_id}'"
    )
    if site["id"] != client_site.id:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=f"client authorized for different site_id than {site_id}",
        )


@router.post(
    "/sites/{site_id}:putObject",
    response_model=Operation[DrsObjectIn, ObjectPutMetadata],
    dependencies=[Depends(verify_client_site_pair)],
)
def put_object_in_site(
    site_id: str,
    object_in: DrsObjectBase,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    s3client: botocore.client.BaseClient = Depends(get_s3_client),
):
    if site_id != API_SITE_ID:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"API-mediated object storage for site {site_id} is not enabled.",
        )
    expires_in = 300
    object_id = generate_one_id(mdb, S3_ID_NS)
    url = presigned_url_to_put(
        f"{S3_ID_NS}/{object_id}",
        client=s3client,
        mime_type=object_in.mime_type,
        expires_in=expires_in,
    )
    # XXX ensures defaults are set, e.g. done:false
    op = Operation[DrsObjectIn, ObjectPutMetadata](
        **{
            "id": generate_one_id(mdb, "op"),
            "expire_time": expiry_dt_from_now(days=30, seconds=expires_in),
            "metadata": {
                "object_id": object_id,
                "site_id": site_id,
                "url": url,
                "expires_in_seconds": expires_in,
                "model": dotted_path_for(ObjectPutMetadata),
            },
        }
    )
    mdb.operations.insert_one(op.dict())
    return op


@router.post(
    "/sites/{site_id}:getObjectLink",
    response_model=AccessURL,
    dependencies=[Depends(verify_client_site_pair)],
)
def get_site_object_link(
    site_id: str,
    access_method: AccessMethod,
    s3client: botocore.client.BaseClient = Depends(get_s3_client),
):
    if site_id != API_SITE_ID:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"API-mediated object storage for site {site_id} is not enabled.",
        )
    url = presigned_url_to_get(
        f"{S3_ID_NS}/{access_method.access_id}",
        client=s3client,
    )
    return {"url": url}


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

    # XXX client_id must not contain a ':' because HTTPBasic auth splits on ':'.
    client_id = local_part(generate_one_id(mdb, "site_clients"))
    client_secret = generate_secret()
    hashed_secret = get_password_hash(client_secret)
    mdb.sites.update_one(
        {"id": site_id},
        {"$push": {"clients": {"id": client_id, "hashed_secret": hashed_secret}}},
    )

    return {
        "client_id": client_id,
        "client_secret": client_secret,
    }
