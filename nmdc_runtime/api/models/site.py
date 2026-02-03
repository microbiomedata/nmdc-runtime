import logging
from typing import List, Optional

import pymongo.database
from fastapi import Depends, HTTPException, status
from jose import jwt
from pydantic import BaseModel, ConfigDict, Field
from jose.exceptions import ExpiredSignatureError, JWTClaimsError, JWTError

from nmdc_runtime.api.core.auth import (
    verify_password,
    TokenData,
    optional_oauth2_scheme,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.user import (
    oauth2_scheme,
    SECRET_KEY,
    ALGORITHM,
)


class Site(BaseModel):
    id: str
    capability_ids: List[str] = []


class SiteClientInDB(BaseModel):
    id: str
    hashed_secret: str


class SiteInDB(Site):
    clients: List[SiteClientInDB] = []


class SiteClientPatchIn(BaseModel):
    """
    A request payload used to patch (i.e. partially update) a site client.
    """

    # Forbid extra fields.
    model_config = ConfigDict(extra="forbid")

    secret: Optional[str] = Field(
        None,
        description="The secret you want the site client to have.",
        min_length=8,
        examples=["new_secret_42!"],
    )


def get_site(mdb, client_id: str) -> Optional[SiteInDB]:
    r"""
    Returns the site, if any, for which the specified `client_id` was generated.
    """

    site = mdb.sites.find_one({"clients.id": client_id})
    if site is not None:
        return SiteInDB(**site)


def authenticate_site_client(mdb, client_id: str, client_secret: str):
    site = get_site(mdb, client_id)
    if not site:
        return False
    hashed_secret = next(
        client.hashed_secret for client in site.clients if client.id == client_id
    )
    if not verify_password(client_secret, hashed_secret):
        return False
    return site


async def get_current_client_site(
    token: str = Depends(oauth2_scheme),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
) -> SiteInDB:
    """
    Returns the site associated with the site client identified by the specified token.

    Raises an exception if the token is invalid.

    Note: This function is similar to the `get_current_user` function
          defined in `nmdc_runtime/api/models/user.py`. Consider consolidating
          the exception definitions (accounting for differences in `detail` strings).
    """

    # Define some exceptions, which contain actionable—but not sensitive—information.
    invalid_subject_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Access token is invalid. Please log in as a site client.",
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
        if subject.startswith("client:"):
            subject_prefix = "client:"
        else:
            logging.warning("The subject contains an invalid prefix.")
            raise invalid_subject_exception
        client_id = subject.removeprefix(subject_prefix)
        if client_id == "":
            logging.warning("The subject contains nothing after the prefix.")
            raise invalid_subject_exception
    else:
        logging.warning("The subject is not a string.")
        raise invalid_subject_exception
    token_data = TokenData(subject=client_id)

    # Get the associated site.
    site = get_site(mdb, client_id=client_id)
    if site is None:
        logging.warning(
            f"Failed to resolve token subject '{token_data.subject}' to a site."
        )
        raise invalid_subject_exception
    return site


async def maybe_get_current_client_site(
    token: str = Depends(optional_oauth2_scheme),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    if token is None:
        return None
    return await get_current_client_site(token, mdb)
