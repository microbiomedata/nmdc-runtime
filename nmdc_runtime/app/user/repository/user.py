from abc import ABCMeta, abstractmethod
from typing import Optional, List

from motor import MotorDatabase

from nmdc_runtime.app.user.domain import User


class UserRepository:
    __metaclass__ = ABCMeta

    @abstractmethod
    async def get_by_id(self, user_id: str) -> Optional[User]:
        pass

    @abstractmethod
    async def get_all(self) -> List[User]:
        pass

    @abstractmethod
    async def save(self, user: User) -> User:
        pass

    @abstractmethod
    async def delete(self, user: User) -> None:
        pass


class UserMongoDBRepository(UserRepository):
    """Concrete Repository for the MongoDB instance"""

    def __init__(self, db: MotorDatabase) -> None:
        self.collection = db["users"]

    async def get_all(self) -> List[User]:
        with self.collection as collection:
            return collection.find({"i": {"$lt": 2}}).to_list(length=100)
