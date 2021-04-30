from fastapi import APIRouter

router = APIRouter()


@router.post("/compute_resources")
def create_compute_resource():
    pass


@router.get("/compute_resources")
def list_compute_resources():
    pass


@router.get("/compute_resources/{compute_resource_id}")
def get_compute_resource():
    pass


@router.patch("/compute_resources/{compute_resource_id}")
def update_compute_resource():
    pass


@router.put("/compute_resources/{compute_resource_id}")
def replace_compute_resource():
    pass


@router.delete("/compute_resources/{compute_resource_id}")
def delete_compute_resource():
    pass
