from fastapi import APIRouter

from . import users

router = APIRouter(
    prefix="/v1", tags=["v1"], responses={404: {"description": "Not found"}}
)

router.include_router(users.router, tags=["users"])
