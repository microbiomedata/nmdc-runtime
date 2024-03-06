"""Endpoints module."""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Response, status
from dependency_injector.wiring import inject, Provide

from nmdc_runtime.containers import Container

from nmdc_runtime.domain.users.userService import UserService
from nmdc_runtime.domain.users.userSchema import UserAuth, UserOut


router = APIRouter(prefix="/users", tags=["users"])


# @router.get("", response_model=Response)
# @inject
# async def index(
#     query: Optional[str] = None,
#     limit: Optional[str] = None,
#     user_service: UserService = Depends(Provide[Container.user_service]),
# ) -> List[UserOut]:
#     query = query
#     limit = limit

#     users = await user_service.search(query, limit)

#     return {"query": query, "limit": limit, "users": users}


@router.post("", response_model=Response, status_code=status.HTTP_201_CREATED)
@inject
async def add(
    user: UserAuth,
    user_service: UserService = Depends(Provide[Container.user_service]),
) -> UserOut:
    new_user = await user_service.create_user(user)
    return new_user
