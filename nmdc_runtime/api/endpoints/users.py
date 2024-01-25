import json
from datetime import timedelta
from typing import Annotated

import pymongo.database
import requests
from fastapi import Depends, APIRouter, HTTPException, status, Cookie
from fastapi.openapi.docs import get_swagger_ui_html
from jose import jws, JWTError
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, PlainTextResponse

from nmdc_runtime.api.core.auth import (
    OAuth2PasswordOrClientCredentialsRequestForm,
    Token,
    ACCESS_TOKEN_EXPIRES,
    create_access_token,
    ORCID_NMDC_CLIENT_ID,
    ORCID_JWK,
    ORCID_JWS_VERITY_ALGORITHM,
    credentials_exception,
    ORCID_NMDC_CLIENT_SECRET,
)
from nmdc_runtime.api.core.auth import get_password_hash
from nmdc_runtime.api.core.util import generate_secret
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import BASE_URL_EXTERNAL
from nmdc_runtime.api.models.site import authenticate_site_client
from nmdc_runtime.api.models.user import UserInDB, UserIn, get_user
from nmdc_runtime.api.models.user import (
    authenticate_user,
    User,
    get_current_active_user,
)

router = APIRouter()


@router.get("/orcid_code", response_class=RedirectResponse, include_in_schema=False)
async def receive_orcid_code(request: Request, code: str, state: str | None = None):
    rv = requests.post(
        "https://orcid.org/oauth/token",
        data=(
            f"client_id={ORCID_NMDC_CLIENT_ID}&client_secret={ORCID_NMDC_CLIENT_SECRET}&"
            f"grant_type=authorization_code&code={code}&redirect_uri={BASE_URL_EXTERNAL}/orcid_code"
        ),
        headers={
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )
    token_response = rv.json()
    response = RedirectResponse(state or request.url_for("custom_swagger_ui_html"))
    for key in ["user_orcid", "user_name", "user_id_token"]:
        response.set_cookie(
            key=key,
            value=token_response[key.replace("user_", "")],
            max_age=2592000,
        )
    return response


@router.get("/orcid_jwt")
async def get_orcid_jwt(user_id_token: Annotated[str | None, Cookie()] = None):
    if user_id_token:
        return PlainTextResponse(content=user_id_token)
    else:
        return PlainTextResponse(content="No ORCiD cookie found. Did you log in?")


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
        # If the HTTP request didn't include a Client Secret, we validate the Client ID as an ORCID JWT.
        # We get a username from that ORCID JWT and fetch the corresponding user record from our database,
        # creating that user record if it doesn't already exist.
        if not form_data.client_secret:
            try:
                payload = jws.verify(
                    form_data.client_id,
                    ORCID_JWK,
                    algorithms=[ORCID_JWS_VERITY_ALGORITHM],
                )
                payload = json.loads(payload.decode())
                issuer: str = payload.get("iss")
                if issuer != "https://orcid.org":
                    raise credentials_exception
                subject: str = payload.get("sub")
                user = get_user(mdb, subject)
                if user is None:
                    mdb.users.insert_one(
                        UserInDB(
                            username=subject,
                            hashed_password=get_password_hash(generate_secret()),
                        ).model_dump(exclude_unset=True)
                    )
                    user = get_user(mdb, subject)
                assert user is not None, "failed to create orcid user"
                access_token_expires = timedelta(**ACCESS_TOKEN_EXPIRES.model_dump())
                access_token = create_access_token(
                    data={"sub": f"user:{user.username}"},
                    expires_delta=access_token_expires,
                )

            except JWTError:
                raise credentials_exception
        else:  # form_data.client_secret
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
