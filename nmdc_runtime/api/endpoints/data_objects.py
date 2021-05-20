from http import HTTPStatus

from fastapi import APIRouter, status

router = APIRouter()


@router.post("/data_objects")
def create_data_object():
    pass


@router.get("/data_objects")
def list_data_objects():
    pass


@router.get("/data_objects/{data_object_id}")
def get_data_object():
    pass


@router.patch("/data_objects/{data_object_id}")
def update_data_object():
    pass


@router.put("/data_objects/{data_object_id}")
def replace_data_object():
    pass
