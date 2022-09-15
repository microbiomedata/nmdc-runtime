from typing import Any, List

import pytest

from src.domain.users.userSchema import (
    UserCreateSchema,
    UserDBSchema,
    UserUpdateSchema,
)
from src.domain.users.userService import UserService
from src.infrastructure.database.models.user import UserModel


USER_MODEL = UserOt(
    username="bob",
    email="test@test.com",
    full_name="test",
    password="test",
    site_admin=["test_site"],
    created_date="1/1/2020",
)


class UserQueriesDummy:
    async def create_user(self, user: Any) -> UserModel:
        return USER_MODEL

    async def update_user(self, old_user: Any, new_user: Any) -> UserModel:
        return USER_MODEL

    async def delete_user(self, user_id: int) -> UserModel:
        return USER_MODEL

    async def get_user_byid(self, user_id: int) -> UserModel:
        return USER_MODEL

    async def get_all_users(self) -> List[UserModel]:
        return [USER_MODEL]


@pytest.fixture
def user_model() -> UserModel:
    return USER_MODEL


@pytest.fixture
def user_schema() -> UserCreateSchema:
    return UserCreateSchema(
        username="bob",
        email="test@test.com",
        full_name="test",
        password="test",
        site_admin=["test_site"],
        created_date="1/1/2020",
    )


@pytest.fixture
def user_update_schema() -> UserUpdateSchema:
    return UserUpdateSchema(
        username="bob",
        email="test@test.com",
        full_name="test",
        password="test",
        site_admin=["test_site"],
        created_date="1/1/2020",
    )
