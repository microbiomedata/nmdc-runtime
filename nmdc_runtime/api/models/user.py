from typing import Optional, List

import pymongo.database
from fastapi import Depends, HTTPException
from jose import JWTError, jwt
from pydantic import BaseModel

from nmdc_runtime.api.core.auth import (
    verify_password,
    SECRET_KEY,
    ALGORITHM,
    oauth2_scheme,
    credentials_exception,
    TokenData,
)
from nmdc_runtime.api.db.mongo import get_mongo_db


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    site_admin: Optional[List[str]] = []
    disabled: Optional[bool] = None


class UserInDB(User):
    hashed_password: str


def get_user(mdb, username: str) -> UserInDB:
    user = mdb.users.find_one({"username": username})
    if user is not None:
        return UserInDB(**user)


def authenticate_user(mdb, username: str, password: str):
    user = get_user(mdb, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
) -> UserInDB:
    if mdb.invalidated_tokens.find_one({"_id": token}):
        raise credentials_exception
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        subject: str = payload.get("sub")
        if subject is None:
            raise credentials_exception
        if not subject.startswith("user:"):
            raise credentials_exception
        username = subject.split("user:", 1)[1]
        token_data = TokenData(subject=username)
    except JWTError:
        raise credentials_exception
    user = get_user(mdb, username=token_data.subject)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: User = Depends(get_current_user),
) -> UserInDB:
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
