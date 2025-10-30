import logging
from typing import List, Optional, Union

import pymongo.database
from fastapi import Depends, HTTPException, status
from jose import jwt
from pydantic import BaseModel
from jose.exceptions import ExpiredSignatureError, JWTClaimsError, JWTError

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

from nmdc_runtime.api.db.mongo import get_mongo_db


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


def get_user(mdb, username: str) -> Optional[UserInDB]:
    r"""
    Returns the user having the specified username.
    """

    user = mdb.users.find_one({"username": username})
    if user is not None:
        return UserInDB(**user)


def authenticate_user(mdb, username: str, password: str) -> Union[UserInDB, bool]:
    r"""
    Returns the user, if any, having the specified username/password combination.
    """

    user = get_user(mdb, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    bearer_credentials: str = Depends(bearer_scheme),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
) -> UserInDB:
    r"""
    Returns a user based upon the provided token.

    If the token belongs to a site client, the returned user is an ephemeral "user"
    whose username is the site client's `client_id`.

    Raises an exception if the token is invalid.

    Reference: The following web page contains information about JWT claims:
               https://auth0.com/docs/secure/tokens/json-web-tokens/json-web-token-claims
    """

    # Define some exceptions, which contain actionable—but not sensitive—information.
    invalid_subject_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token is invalid. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    invalid_claims_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token is invalid. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    invalid_token_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token is invalid. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    invalidated_token_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token has been invalidated. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    expired_token_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token has expired. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    invalid_or_missing_token_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token is invalid or missing. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # Check whether there is a token, and whether it has been invalidated.
    if token is None:
        raise invalid_or_missing_token_exception
    elif mdb.invalidated_tokens.find_one({"_id": token}):
        raise invalidated_token_exception

    # Validate the signature of the JWT and extract its payload.
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except ExpiredSignatureError as e:
        logging.exception(e)
        raise expired_token_exception
    except JWTClaimsError as e:
        logging.exception(e)
        raise invalid_claims_exception
    except (JWTError, AttributeError) as e:
        logging.exception(e)
        raise invalid_token_exception

    # Extract the prefix and the username from the subject.
    subject: Optional[str] = payload.get("sub", None)
    if isinstance(subject, str):
        if subject.startswith("user:"):
            subject_prefix = "user:"
        elif subject.startswith("client:"):
            subject_prefix = "client:"
        else:
            logging.warning("The subject contains an invalid prefix.")
            raise invalid_subject_exception
        username = subject.removeprefix(subject_prefix)
        if username == "":
            logging.warning("The subject contains nothing after the prefix.")
            raise invalid_subject_exception
    else:
        logging.warning("The subject is not a string.")
        raise invalid_subject_exception
    token_data = TokenData(subject=username)

    # Coerce a "client" into a "user"
    # TODO: consolidate the client/user distinction.
    if not isinstance(token_data.subject, str):
        logging.warning("The subject is not a string.")
        raise invalid_subject_exception
    elif subject_prefix == "user:":
        user = get_user(mdb, username=token_data.subject)
    elif subject_prefix == "client:":
        # construct a user from the client_id
        user = get_client_user(mdb, client_id=token_data.subject)
    else:
        # Note: We already validate the subject's prefix above, so we expect this case to never occur.
        logging.warning("The subject prefix is not something we recognize.")
        user = None

    if user is None:
        logging.warning(
            f"Failed to resolve token subject '{token_data.subject}' to a user."
        )
        raise invalid_subject_exception
    return user


def get_client_user(mdb, client_id: str) -> UserInDB:
    r"""
    Returns an ephemeral "user" whose username is the specified `client_id`
    and whose password is the hashed secret of the client; provided that the
    specified `client_id` is associated with a site in the database.

    TODO: Clarify the above summary of the function.
    """

    # Get the site associated with the identified client.
    site = get_site(mdb, client_id)
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
