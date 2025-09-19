from typing import Annotated

import pymongo
from fastapi import APIRouter, Depends, status, HTTPException, Query, Path
from toolz import get_in, merge, assoc

from nmdc_runtime.api.core.util import raise404_if_none, pick
from nmdc_runtime.api.db.mongo import get_mongo_db
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


@router.get(
    "/operations", 
    response_model=ListOperationsResponse[ResultT, MetadataT],
    description="List operations with optional filtering",
)
def list_operations(
    req: Annotated[ListRequest, Query()],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Retrieve a list of operations with optional filtering and pagination.
    
    Operations track the progress and status of long-running tasks.
    """
    return list_resources(req, mdb, "operations")


@router.get(
    "/operations/{op_id}", 
    response_model=Operation[ResultT, MetadataT],
    description="Get details of a specific operation",
)
def get_operation(
    op_id: Annotated[
        str,
        Path(
            title="Operation ID",
            description="The unique identifier of the operation.",
            examples=["nmdc:op-11-abc123"],
        ),
    ],
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Retrieve detailed information about a specific operation.
    
    Returns operation status, progress, and result data.
    """
    op = raise404_if_none(mdb.operations.find_one({"id": op_id}))
    return op


@router.patch(
    "/operations/{op_id}", 
    response_model=Operation[ResultT, MetadataT],
    description="Update operation status and metadata",
)
def update_operation(
    op_id: Annotated[
        str,
        Path(
            title="Operation ID",
            description="The unique identifier of the operation to update.",
            examples=["nmdc:op-11-abc123"],
        ),
    ],
    op_patch: UpdateOperationRequest,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
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
    doc_op = raise404_if_none(mdb.operations.find_one({"id": op_id}))
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
    mdb.operations.replace_one({"id": op_id}, doc_op_patched)
    return doc_op_patched
