from fastapi import APIRouter

router = APIRouter()


@router.post("/capabilities")
def create_capability():
    pass


@router.get("/capabilities")
def list_capabilities():
    pass


@router.get("/capabilities/{capability_id}")
def get_capability(capability_id: str):
    return {"@id": capability_id}


@router.patch("/capabilities/{capability_id}")
def update_capability():
    pass


@router.put("/capabilities/{capability_id}")
def replace_capability():
    pass
