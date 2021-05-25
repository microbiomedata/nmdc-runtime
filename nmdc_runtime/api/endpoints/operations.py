import pymongo
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.operation import (
    ListOperationsResponse,
    ResultT,
    MetadataT,
    ListOperationsRequest,
    Operation,
)

router = APIRouter()


@router.get("/operations", response_model=ListOperationsResponse[ResultT, MetadataT])
def list_operations(
    req: ListOperationsRequest = Depends(),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    limit = req.max_page_size
    return {"resources": list(mdb.operations.find(limit=limit))}


@router.get("/operations/{op_id}", response_model=Operation[ResultT, MetadataT])
def get_operation(
    op_id: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    op = mdb.operations.find_one({"id": op_id})
    if op is not None:
        return op
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=None)


@router.patch("/operations/{op_id}")
def update_operation():
    pass


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
