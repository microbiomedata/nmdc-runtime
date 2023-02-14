from fastapi import APIRouter

# from . import users
from . import outputs
from .workflows import activities

router_v1 = APIRouter(prefix="/v1", responses={404: {"description": "Not found"}})

router_v1.include_router(activities.router)
