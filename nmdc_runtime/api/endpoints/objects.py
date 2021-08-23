import os
from typing import List

import botocore
import pymongo
from fastapi import APIRouter, status, Depends, HTTPException
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from starlette.responses import RedirectResponse

from nmdc_runtime.api.core.idgen import decode_id, generate_one_id, local_part
from nmdc_runtime.api.core.util import raise404_if_none, API_SITE_ID
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.db.s3 import S3_ID_NS, presigned_url_to_get, get_s3_client
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.object import (
    DrsId,
    DrsObject,
    DrsObjectIn,
    AccessURL,
)
from nmdc_runtime.api.models.object_type import ObjectType, DrsObjectWithTypes
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.api.models.util import ListRequest, ListResponse

router = APIRouter()

BASE_URL_EXTERNAL = os.getenv("API_HOST_EXTERNAL")
HOSTNAME_EXTERNAL = BASE_URL_EXTERNAL.split("://", 1)[-1]


def supplied_object_id(mdb, client_site, obj_doc):
    if "access_methods" not in obj_doc:
        return None
    for method in obj_doc["access_methods"]:
        if method.get("access_id") and ":" in method["access_id"]:
            site_id, _, object_id = method["access_id"].rpartition(":")
            if (
                client_site.id == site_id
                and mdb.sites.count_documents({"id": site_id})
                and mdb.ids.count_documents(
                    {"_id": decode_id(object_id), "ns": S3_ID_NS}
                )
                and mdb.objects.count_documents({"id": object_id}) == 0
            ):
                return object_id
    return None


@router.post("/objects", status_code=status.HTTP_201_CREATED, response_model=DrsObject)
def create_object(
    object_in: DrsObjectIn,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    client_site: Site = Depends(get_current_client_site),
):
    """Create a new DrsObject.

    You may create a *blob* or a *bundle*.

    A *blob* is like a file - it's a single blob of bytes, so there there is no `contents` array,
    only one or more `access_methods`.

    A *bundle* is like a folder - it's a gathering of other objects (blobs and/or bundles) in a
    `contents` array, and `access_methods` is optional because a data consumer can fetch each of
    the bundle contents individually.

    At least one checksum is required. The names of supported checksum types are given by
    the set of Python 3.8 `hashlib.algorithms_guaranteed`:
    > blake2b | blake2s | md5 | sha1 | sha224 | sha256 | sha384 | sha3_224 | sha3_256 | sha3_384 | sha3_512 | sha512 | shake_128 | shake_256

    Each provided `access_method` needs either an `access_url` or an `access_id`.

    """
    id_supplied = supplied_object_id(
        mdb, client_site, object_in.dict(exclude_unset=True)
    )
    drs_id = local_part(
        id_supplied if id_supplied is not None else generate_one_id(mdb, S3_ID_NS)
    )
    self_uri = f"drs://{HOSTNAME_EXTERNAL}/{drs_id}"
    print(drs_id)
    drs_obj = DrsObject(
        **object_in.dict(exclude_unset=True), id=drs_id, self_uri=self_uri
    )
    doc = drs_obj.dict(exclude_unset=True)
    doc["_mgr_site"] = client_site.id  # manager site
    try:
        mdb.objects.insert_one(doc)
    except DuplicateKeyError as e:
        if e.details["keyPattern"] == {"checksums.type": 1, "checksums.checksum": 1}:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="provided checksum matches existing object",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="duplicate key error",
            )
    return doc


@router.get("/objects", response_model=ListResponse[DrsObject])
def list_objects(
    req: ListRequest = Depends(),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return list_resources(req, mdb, "objects")


@router.get(
    "/objects/{object_id}", response_model=DrsObject, response_model_exclude_unset=True
)
def get_object_info(
    object_id: DrsId,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    return raise404_if_none(mdb.objects.find_one({"id": object_id}))


@router.get(
    "/ga4gh/drs/v1/objects/{object_id}",
    summary="Get Object Info",
    response_model=DrsObject,
    responses={
        status.HTTP_303_SEE_OTHER: {
            "description": "See other",
            "headers": {"Location": {"schema": {"type": "string"}}},
        },
    },
)
def get_ga4gh_object_info(object_id: DrsId):
    """Redirect to /objects/{object_id}."""
    return RedirectResponse(
        BASE_URL_EXTERNAL + f"/objects/{object_id}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/objects/{object_id}/types", response_model=List[ObjectType])
def list_object_types(object_id: DrsId):
    return object_id


@router.put("/objects/{object_id}/types", response_model=DrsObjectWithTypes)
def replace_object_types(
    object_id: str,
    object_type_ids: List[str],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    unknown_type_ids = set(object_type_ids) - set(mdb.object_types.distinct("id"))
    if unknown_type_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unknown type ids: {unknown_type_ids}.",
        )
    doc_after = mdb.objects.find_one_and_update(
        {"id": object_id},
        {"$set": {"types": object_type_ids}},
        return_document=ReturnDocument.AFTER,
    )
    return doc_after


def object_access_id_ok(obj_doc, access_id):
    if "access_methods" not in obj_doc:
        return False
    for method in obj_doc["access_methods"]:
        if method.get("access_id") and method["access_id"] == access_id:
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
