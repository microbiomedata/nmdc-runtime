from typing import List, Optional


from fastapi import Depends
from jose import JWTError, jwt
from pydantic import BaseModel

from nmdc_runtime.mongo_util import get_runtime_mdb, RuntimeAsyncMongoDatabase
from nmdc_runtime.api.core.auth import (
    verify_password,
    TokenData,
    optional_oauth2_scheme,
)
from nmdc_runtime.api.models.user import (
    oauth2_scheme,
    credentials_exception,
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


async def get_site(
    mdb: RuntimeAsyncMongoDatabase, client_id: str
) -> Optional[SiteInDB]:
    r"""
    Returns the site, if any, for which the specified `client_id` was generated.
    """

    site = await mdb.raw["sites"].find_one({"clients.id": client_id})
    return SiteInDB(**site) if site is not None else None


async def authenticate_site_client(mdb, client_id: str, client_secret: str):
    site = await get_site(mdb, client_id)
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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    if await mdb.raw["invalidated_tokens"].find_one({"_id": token}):
        raise credentials_exception
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        subject: str = payload.get("sub")
        if subject is None:
            raise credentials_exception
        if not subject.startswith("client:"):
            raise credentials_exception
        client_id = subject.split("client:", 1)[1]
        token_data = TokenData(subject=client_id)
    except JWTError:
        raise credentials_exception
    site = await get_site(mdb, client_id=token_data.subject)
    if site is None:
        raise credentials_exception
    return site


async def maybe_get_current_client_site(
    token: str = Depends(optional_oauth2_scheme),
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    if token is None:
        return None
    return await get_current_client_site(token, mdb)
