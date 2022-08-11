from typing import Any

from nmdc_runtime.domain.users.userSchema import UserAuth, UserUpdate, UserOut


class UserService:
    def __init__(self, user_queries: Any) -> None:
        self.__user_queries = user_queries

    async def create_user(self, user: UserAuth) -> UserOut:
        return await self.__user_queries.create(user)

    async def update_user(self, username: str, new_user: UserUpdate) -> UserOut:
        pass
