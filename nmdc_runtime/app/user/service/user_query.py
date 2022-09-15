from typing import Optional

from app.user.exception.user import UserNotFoundException
from app.user.repository import UserRepository
from app.user.schema import UserSchema


class UserService:
    def __init__(self, user_repo: UserRepository) -> None:
        self.__repository: UserRepository = user_repo

    async def get_user(self, user_id: str) -> Optional[UserSchema]:
        user = await self.user_repo.get_by_id(user_id=user_id)
        if not user:
            raise UserNotFoundException

        return UserSchema.from_orm(user)

    async def is_site_admin(self, user_id: str) -> bool:
        user = await self.user_repo.get_by_id(user_id=user_id)
        if not user:
            return False

        if user.is_admin is False:
            return False

        return True
