from fastapi import APIRouter

router_v1 = APIRouter(prefix="/v1", responses={404: {"description": "Not found"}})
