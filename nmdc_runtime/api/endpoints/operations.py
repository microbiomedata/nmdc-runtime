from fastapi import APIRouter

router = APIRouter()


@router.get("/operations")
def list_operations():
    pass


@router.get("/operations/{op_id}")
def get_operation():
    pass


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
