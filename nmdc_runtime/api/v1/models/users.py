from typing import Optional, List

from pydantic import BaseModel


from nmdc_runtime.domain.users.userSchema import UserOut


class Response(BaseModel):
    query: str
    limit: int


class UserResponse(Response):
    users: List[UserOut]
