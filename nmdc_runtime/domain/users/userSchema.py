from typing import Optional, List


from pydantic import BaseModel, EmailStr


class UserBase(BaseModel):
    username: Optional[str] = None
    email: Optional[str] = None
    full_name: Optional[str] = None
    site_admin: Optional[List[str]] = []
    disabled: Optional[bool] = False


class UserAuth(UserBase):
    """User register and login auth"""

    username: str
    password: str


# Properties to receive via API on update
class UserUpdate(UserBase):
    """Updatable user fields"""

    email: Optional[EmailStr] = None

    # User information
    full_name: Optional[str] = None
    password: Optional[str] = None


class UserOut(UserUpdate):
    """User fields pushed to the client"""

    email: EmailStr
    disabled: Optional[bool] = False
