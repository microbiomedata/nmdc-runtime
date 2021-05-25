from fastapi import APIRouter

router = APIRouter()


@router.post("/object_types")
def create_object_type():
    pass


@router.get("/object_types")
def list_object_types():
    pass
