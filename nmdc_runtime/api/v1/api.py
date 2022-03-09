from fastapi import APIRouter

from nmdc_runtime.api.v1.endpoints import jobs

v1api_router = APIRouter()
v1api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
