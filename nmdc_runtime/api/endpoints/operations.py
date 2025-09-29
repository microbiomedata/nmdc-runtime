from typing import Annotated

from fastapi import APIRouter, Depends, status, HTTPException, Query
from toolz import get_in, merge, assoc

from nmdc_runtime.mongo_util import get_runtime_mdb, RuntimeAsyncMongoDatabase
from nmdc_runtime.api.core.util import raise404_if_none, pick
from nmdc_runtime.api.endpoints.util import list_resources
from nmdc_runtime.api.models.operation import (
    ListOperationsResponse,
    ResultT,
    MetadataT,
    Operation,
    UpdateOperationRequest,
)
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.api.models.util import ListRequest

router = APIRouter()


@router.get("/operations", response_model=ListOperationsResponse[ResultT, MetadataT])
def list_operations(
    req: Annotated[ListRequest, Query()],
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    return list_resources(req, mdb, "operations")


@router.get("/operations/{op_id}", response_model=Operation[ResultT, MetadataT])
def get_operation(
    op_id: str,
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    op = raise404_if_none(mdb.operations.find_one({"id": op_id}))
    return op


@router.patch("/operations/{op_id}", response_model=Operation[ResultT, MetadataT])
async def update_operation(
    op_id: str,
    op_patch: UpdateOperationRequest,
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
    client_site: Site = Depends(get_current_client_site),
):
    """

    A site client can update an operation if and only if its site_id is the operation's
    `metadata.site_id`.

    The following fields in `metadata` are used by the system and are read-only:
    - site_id
    - job
    - model
    """
    # TODO be able to make job "undone" and "redone" to re-trigger downstream ETL.
    doc_op = raise404_if_none(await mdb.raw["operations"].find_one({"id": op_id}))
    site_id_op = get_in(["metadata", "site_id"], doc_op)
    if site_id_op != client_site.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"client authorized for different site_id than {site_id_op}",
        )
    op_patch_metadata = merge(
        op_patch.model_dump(exclude_unset=True).get("metadata", {}),
        pick(["site_id", "job", "model"], doc_op.get("metadata", {})),
    )
    doc_op_patched = merge(
        doc_op,
        assoc(
            op_patch.model_dump(exclude_unset=True),
            "metadata",
            op_patch_metadata,
        ),
    )
    await mdb.raw["operations"].replace_one({"id": op_id}, doc_op_patched)
    return doc_op_patched
