import json

import pymongo
from fastapi import APIRouter, Depends, status, HTTPException
from toolz import get_in, merge

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.operation import (
    ListOperationsResponse,
    ResultT,
    MetadataT,
    ListOperationsRequest,
    Operation,
    UpdateOperationRequest,
)
from nmdc_runtime.api.models.site import Site, get_current_client_site

router = APIRouter()


@router.get("/operations", response_model=ListOperationsResponse[ResultT, MetadataT])
def list_operations(
    req: ListOperationsRequest = Depends(),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    limit = req.max_page_size
    filter_ = json.loads(req.filter) if req.filter else {}
    print(req.dict())
    if req.page_token:
        doc = mdb.page_tokens.find_one({"_id": req.page_token, "ns": "operations"})
        if doc is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Bad page_token"
            )
        last_id = doc["id"]
        mdb.page_tokens.delete_one({"_id": req.page_token})
    else:
        last_id = None
    if last_id is not None:
        if "id" in filter_:
            filter_["id"] = merge(filter_["id"], {"$gt": last_id})
        else:
            filter_ = merge(filter_, {"id": {"$gt": last_id}})

    if mdb.operations.count_documents(filter=filter_) <= limit:
        return {"resources": list(mdb.operations.find(filter=filter_))}
    else:
        resources = list(
            mdb.operations.find(filter=filter_, limit=limit, sort=[("id", 1)])
        )
        last_id = resources[-1]["id"]
        token = generate_id_unique(mdb, "page_tokens")
        mdb.page_tokens.insert_one({"_id": token, "ns": "operations", "id": last_id})
        return {"resources": resources, "next_page_token": token}


@router.get("/operations/{op_id}", response_model=Operation[ResultT, MetadataT])
def get_operation(
    op_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    op = raise404_if_none(mdb.operations.find_one({"id": op_id}))
    return op


@router.patch("/operations/{op_id}", response_model=Operation[ResultT, MetadataT])
def update_operation(
    op_id: str,
    op_patch: UpdateOperationRequest,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    client_site: Site = Depends(get_current_client_site),
):
    doc_op = raise404_if_none(mdb.operations.find_one({"id": op_id}))
    # A site client can update operation iff its site_id is in op metadata.
    site_id_op = get_in(["metadata", "site_id"], doc_op)
    if site_id_op != client_site.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"client authorized for different site_id than {site_id_op}",
        )
    doc_op_patched = merge(doc_op, op_patch.dict(exclude_unset=True))
    mdb.operations.replace_one({"id": op_id}, doc_op_patched)
    return doc_op_patched


@router.post(
    "/operations/{op_id}:wait",
    description=(
        "Wait until the operation is resolved or rejected before returning the result."
        " This is a 'blocking' alternative to client-side polling, and may not be available"
        " for operation types know to be particularly long-running."
    ),
)
def wait_operation():
    pass


@router.post("/operations/{op_id}:cancel")
def cancel_operation():
    pass


@router.post("/operations/{op_id}:pause")
def pause_operation():
    pass


@router.post("/operations/{op_id}:resume")
def resume_operation():
    pass
