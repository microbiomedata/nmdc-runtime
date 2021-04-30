from fastapi import APIRouter

router = APIRouter()


@router.post("/data_resources")
def create_data_resource():
    pass


@router.get("/data_resources")
def list_data_resources():
    pass


@router.get("/data_resources/{data_resource_id}")
def get_data_resource():
    pass


@router.patch("/data_resources/{data_resource_id}")
def update_data_resource():
    pass


@router.put("/data_resources/{data_resource_id}")
def replace_data_resource():
    pass


@router.delete("/data_resources/{data_resource_id}")
def delete_data_resource():
    pass
