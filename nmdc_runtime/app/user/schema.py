from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field, EmailStr


class UserBase(BaseModel):
    password: str = Field(None, description="Password")
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    site_admin: Optional[List[str]] = []
    disabled: Optional[bool] = False
    created_at: datetime = Field(None, description="Create Time")
    updated_at: datetime = Field(None, description="Update Time")


class UserCreateSchema(UserBase):
    email: EmailStr
    password: str


class UserUpdateSchema(UserBase):
    password: Optional[str] = None


class UserDBSchema(UserBase):
    __collection__ = "users"
    username: str = Field(None, description="ID")

    class Config:
        orm_mode = True
