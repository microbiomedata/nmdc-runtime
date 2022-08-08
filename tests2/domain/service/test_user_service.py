from typing import Any, List

import pytest

from nmdc_runtime.domain.users.queriesInterface import IUserQueries
from nmdc_runtime.domain.users.userSchema import (
    UserAuth,
    UserUpdate,
    UserOut,
)

from nmdc_runtime.domain.users.userService import UserService

USER_OUT = UserOut(
    email="test+email@test.com",
)


class UserQueriesDummy(IUserQueries):
    async def create(self, user: Any) -> UserOut:
        return USER_OUT

    async def update(self, old_user: Any, new_user: Any) -> UserOut:
        return USER_OUT


@pytest.fixture
def user_out() -> UserOut:
    return USER_OUT


@pytest.fixture
def user_schema() -> UserAuth:
    return UserAuth(
        username="bob",
        password="test",
    )


@pytest.fixture
def user_update_schema() -> UserUpdate:
    return UserUpdate(
        email="test@test.com",
        full_name="test",
        password="test",
    )


class TestUserService:
    @pytest.mark.asyncio
    async def test_user_create_valid(
        self, user_out: UserOut, user_schema: UserAuth
    ) -> None:
        user_service = UserService(UserQueriesDummy())

        result = await user_service.create_user(user_schema)
        assert result == UserOut(email="test+email@test.com")
