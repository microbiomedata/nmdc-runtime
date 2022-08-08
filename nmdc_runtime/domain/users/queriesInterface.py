from __future__ import annotations
from abc import ABC

from abc import abstractmethod

from nmdc_runtime.domain.users.userSchema import UserAuth, UserUpdate, UserOut


class IUserQueries(ABC):
    @abstractmethod
    async def create(self, user: UserAuth) -> UserOut:
        """Create new user"""
        raise NotImplementedError

    @abstractmethod
    async def update(self, user: UserUpdate) -> UserOut:
        """Update user data"""
        raise NotImplementedError
