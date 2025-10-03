from typing import List, Optional, Union


from fastapi import Depends, HTTPException
from jose import JWTError, jwt
from pydantic import BaseModel

from nmdc_runtime.mongo_util import get_runtime_mdb, RuntimeAsyncMongoDatabase
from nmdc_runtime.api.core.auth import (
    verify_password,
    SECRET_KEY,
    ALGORITHM,
    oauth2_scheme,
    credentials_exception,
    TokenData,
    bearer_scheme,
)
from nmdc_runtime.api.models.site import get_site


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    site_admin: Optional[List[str]] = []
    disabled: Optional[bool] = False


class UserIn(User):
    password: str


class UserInDB(User):
    hashed_password: str


async def get_user(mdb: RuntimeAsyncMongoDatabase, username: str) -> Optional[UserInDB]:
    r"""
    Returns the user having the specified username.
    """

    user = await mdb.raw["users"].find_one({"username": username})
    return UserInDB(**user) if user is not None else None


async def authenticate_user(mdb, username: str, password: str) -> Union[UserInDB, bool]:
    r"""
    Returns the user, if any, having the specified username/password combination.
    """

    user = await get_user(mdb, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    bearer_credentials: str = Depends(bearer_scheme),
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
) -> UserInDB:
    r"""
    Returns a user based upon the provided token.

    If the token belongs to a site client, the returned user is an ephemeral "user"
    whose username is the site client's `client_id`.

    Raises an exception if the token is invalid.
    """

    if await mdb.raw["invalidated_tokens"].find_one({"_id": token}):
        raise credentials_exception
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        subject: str = payload.get("sub")
        if subject is None:
            raise credentials_exception
        if not subject.startswith("user:") and not subject.startswith("client:"):
            raise credentials_exception

        # subject is in the form "user:foo" or "client:bar"
        username = subject.split(":", 1)[1]
        token_data = TokenData(subject=username)
    except (JWTError, AttributeError) as e:
        print(f"jwt error: {e}")
        raise credentials_exception

    # Coerce a "client" into a "user"
    # TODO: consolidate the client/user distinction.
    if subject.startswith("user:"):
        user = await get_user(mdb, username=token_data.subject)
    elif subject.startswith("client:"):
        # construct a user from the client_id
        user = await get_client_user(mdb, client_id=token_data.subject)
    else:
        raise credentials_exception
    if user is None:
        raise credentials_exception
    return user


async def get_client_user(mdb: RuntimeAsyncMongoDatabase, client_id: str) -> UserInDB:
    r"""
    Returns an ephemeral "user" whose username is the specified `client_id`
    and whose password is the hashed secret of the client; provided that the
    specified `client_id` is associated with a site in the database.

    TODO: Clarify the above summary of the function.
    """

    # Get the site associated with the identified client.
    site = await get_site(mdb, client_id)
    if site is None:
        raise credentials_exception

    # Get the client, itself, via the site.
    client = next(client for client in site.clients if client.id == client_id)
    if client is None:
        raise credentials_exception

    # Make an ephemeral "user" whose username matches the client's `id`.
    user = UserInDB(username=client.id, hashed_password=client.hashed_secret)
    return user


async def get_current_active_user(
    current_user: UserInDB = Depends(get_current_user),
) -> UserInDB:
    r"""
    Returns the current user, provided their user account is not disabled.
    """

    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
