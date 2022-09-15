from fastapi import APIRouter

# from . import users
from . import outputs

router_v1 = APIRouter(
    prefix="/v1", tags=["v1"], responses={404: {"description": "Not found"}}
)

# router_v1.include_router(users.router, tags=["users"])
router_v1.include_router(outputs.router, tags=["outputs"])
