import os
from typing import Union, List

import botocore
import pymongo
from fastapi import APIRouter, Response, status, Depends, HTTPException

from nmdc_runtime.api.core.idgen import generate_id_unique, decode_id
from nmdc_runtime.api.core.util import raise404_if_none, API_SITE_ID
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.db.s3 import get_s3_client, S3_ID_NS, presigned_url_to_get
from nmdc_runtime.api.models.object import (
    DrsId,
    Error,
    DrsObject,
    AccessURL,
    DrsObjectIn,
)
from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.api.models.user import User, get_current_active_user

router = APIRouter()

HOSTNAME = os.getenv("API_HOST").split("://", 1)[-1]


def object_id_given(mdb, obj_doc):
    # TODO okay?
    #   Hold up. Don't create object after s3 upload via POST /objects ;
    #   Create it via PATCH /operations/{op_id} to return value of sites.put_object_in_site.
    if "access_methods" not in obj_doc:
        return None
    for method in obj_doc["access_methods"]:
        if "access_id" in method and ":" in method["access_id"]:
            site_id, object_id = method["access_id"].split(":", 1)
            if mdb.sites.count_documents({"id": site_id}) and mdb.ids.count_documents(
                {"_id": decode_id(object_id), "ns": S3_ID_NS}
            ):
                return object_id
    return None


@router.post("/objects", status_code=201, response_model=DrsObject)
def create_object(
    object_in: DrsObjectIn,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    drs_id = generate_id_unique(mdb, S3_ID_NS)
    self_uri = f"drs://{HOSTNAME}/{drs_id}"
    drs_obj = DrsObject(**object_in, id=drs_id, self_uri=self_uri)
    doc = drs_obj.dict()
    doc["_user"] = user.username
    mdb.objects.insert_one(doc)
    return drs_obj


@router.get("/objects", response_model=List[DrsObject])
def list_objects():
    pass


@router.get("/objects/{object_id}", response_model=DrsObject)
def get_object_info(object_id: DrsId):
    return object_id


@router.get("/objects/{object_id}/types", response_model=List[ObjectType])
def list_object_types(object_id: DrsId):
    return object_id


@router.put("/objects/{object_id}/types", response_model=List[ObjectType])
def replace_object_types(object_type_ids: List[str]):
    return object_type_ids


def object_access_id_ok(obj_doc, access_id):
    if "access_methods" not in obj_doc:
        return False
    for method in obj_doc["access_methods"]:
        if "access_id" in method and method["access_id"] == access_id:
            return True
    return False


@router.get("/objects/{object_id}/access/{access_id}", response_model=AccessURL)
def get_object_access(
    object_id: DrsId,
    access_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    s3client: botocore.client.BaseClient = Depends(get_s3_client),
):
    obj_doc = raise404_if_none(mdb.objects.find_one({"id": object_id}))
    if not object_access_id_ok(obj_doc, access_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="access_id not referenced by object",
        )
    if access_id.startswith(f"{API_SITE_ID}:"):
        url = presigned_url_to_get(
            f"{S3_ID_NS}/{object_id}",
            client=s3client,
        )
        return {"url": url}
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="no site found to handle access_id for object",
    )


@router.patch("/objects/{object_id}", response_model=DrsObject)
def update_object():
    pass


@router.put("/objects/{object_id}", response_model=DrsObject)
def replace_object():
    pass
