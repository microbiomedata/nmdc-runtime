"""
User models
"""

from typing import Optional, List

from beanie import Document, Indexed
from pydantic import ConfigDict, EmailStr

from nmdc_runtime.api.core.auth import verify_password
from nmdc_runtime.domain.users.userSchema import UserAuth, UserUpdate, UserOut
from nmdc_runtime.domain.users.queriesInterface import IUserQueries


# User database representation
class User(Document):
    class DocumentMeta:
        collection_name = "users"

    username: Indexed(str, unique=True)
    email: Indexed(EmailStr, unique=True)
    full_name: Optional[str] = None
    site_admin: Optional[List[str]] = []
    disabled: Optional[bool] = False
    model_config = ConfigDict(
        json_schema_extra={
            "username": "bob",
            "email": "test@test.com",
            "full_name": "test",
            "password": "test",
            "site_admin": ["test_site"],
            "created_date": "1/1/2020",
        }
    )


class UserQueries(IUserQueries):
    """Implementation of the User query interface"""

    async def create(self, user: UserAuth) -> UserOut:
        auth_user = await User.get(user.username)
        if not auth_user:
            auth_user = User(
                username=user.username,
                email=user.email,
                full_name=user.full_name,
                site_admin=user.site_admin,
                password=user.password,
            )
            await auth_user.insert()

        if not verify_password(user.password, auth_user.password):
            return False
        return UserOut(auth_user)

    async def update(self, user: UserUpdate) -> UserOut:
        pass
