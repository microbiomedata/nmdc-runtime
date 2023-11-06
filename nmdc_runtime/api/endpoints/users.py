from datetime import timedelta

import pymongo.database
from fastapi import Depends, APIRouter, HTTPException, status

from nmdc_runtime.api.core.auth import (
    OAuth2PasswordOrClientCredentialsRequestForm,
    Token,
    ACCESS_TOKEN_EXPIRES,
    create_access_token,
)
from nmdc_runtime.api.core.auth import get_password_hash
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.site import authenticate_site_client
from nmdc_runtime.api.models.user import UserInDB, UserIn
from nmdc_runtime.api.models.user import (
    authenticate_user,
    User,
    get_current_active_user,
)

router = APIRouter()


@router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordOrClientCredentialsRequestForm = Depends(),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    if form_data.grant_type == "password":
        user = authenticate_user(mdb, form_data.username, form_data.password)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token_expires = timedelta(**ACCESS_TOKEN_EXPIRES.model_dump())
        access_token = create_access_token(
            data={"sub": f"user:{user.username}"}, expires_delta=access_token_expires
        )
    else:  # form_data.grant_type == "client_credentials"
        site = authenticate_site_client(
            mdb, form_data.client_id, form_data.client_secret
        )
        if not site:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect client_id or client_secret",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # TODO make below an absolute time
        access_token_expires = timedelta(**ACCESS_TOKEN_EXPIRES.model_dump())
        access_token = create_access_token(
            data={"sub": f"client:{form_data.client_id}"},
            expires_delta=access_token_expires,
        )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires": ACCESS_TOKEN_EXPIRES.model_dump(),
    }


@router.get("/users/me", response_model=User, response_model_exclude_unset=True)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user


def check_can_create_user(requester: User):
    if "nmdc-runtime-useradmin" not in requester.site_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="only admins for site nmdc-runtime-useradmin are allowed to create users.",
        )


@router.post("/users", status_code=status.HTTP_201_CREATED, response_model=User)
def create_user(
    user_in: UserIn,
    requester: User = Depends(get_current_active_user),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    check_can_create_user(requester)
    mdb.users.insert_one(
        UserInDB(
            **user_in.model_dump(),
            hashed_password=get_password_hash(user_in.password),
        ).model_dump(exclude_unset=True)
    )
    return mdb.users.find_one({"username": user_in.username})
