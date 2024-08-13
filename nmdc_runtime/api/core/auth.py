import os
from datetime import datetime, timedelta
from typing import Optional, Dict

from fastapi import Depends
from fastapi.exceptions import HTTPException
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.param_functions import Form
from fastapi.security import (
    OAuth2,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
    HTTPAuthorizationCredentials,
)
from fastapi.security.utils import get_authorization_scheme_param
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from starlette import status
from starlette.requests import Request
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED

ORCID_PRODUCTION_BASE_URL = "https://orcid.org"

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"
ORCID_NMDC_CLIENT_ID = os.getenv("ORCID_NMDC_CLIENT_ID")
ORCID_NMDC_CLIENT_SECRET = os.getenv("ORCID_NMDC_CLIENT_SECRET")
ORCID_BASE_URL = os.getenv("ORCID_BASE_URL", default=ORCID_PRODUCTION_BASE_URL)

# Define the JSON Web Key Set (JWKS) for ORCID.
#
# Note: The URL from which we got this dictionary is: https://orcid.org/oauth/jwks
#       We got _that_ URL from the dictionary at: https://orcid.org/.well-known/openid-configuration
#
# TODO: Consider _live-loading_ this dictionary from the Internet.
#
ORCID_JWK = {
    "e": "AQAB",
    "kid": "production-orcid-org-7hdmdswarosg3gjujo8agwtazgkp1ojs",
    "kty": "RSA",
    "n": "jxTIntA7YvdfnYkLSN4wk__E2zf_wbb0SV_HLHFvh6a9ENVRD1_rHK0EijlBzikb-1rgDQihJETcgBLsMoZVQqGj8fDUUuxnVHsuGav_bf41PA7E_58HXKPrB2C0cON41f7K3o9TStKpVJOSXBrRWURmNQ64qnSSryn1nCxMzXpaw7VUo409ohybbvN6ngxVy4QR2NCC7Fr0QVdtapxD7zdlwx6lEwGemuqs_oG5oDtrRuRgeOHmRps2R6gG5oc-JqVMrVRv6F9h4ja3UgxCDBQjOVT1BFPWmMHnHCsVYLqbbXkZUfvP2sO1dJiYd_zrQhi-FtNth9qrLLv3gkgtwQ",
    "use": "sig",
}
# If the application is using a _non-production_ ORCID environment, overwrite
# the "kid" and "n" values with those from the sandbox ORCID environment.
#
# Source: https://sandbox.orcid.org/oauth/jwks
#
if ORCID_BASE_URL != ORCID_PRODUCTION_BASE_URL:
    ORCID_JWK["kid"] = "sandbox-orcid-org-3hpgosl3b6lapenh1ewsgdob3fawepoj"
    ORCID_JWK["n"] = (
        "pl-jp-kTAGf6BZUrWIYUJTvqqMVd4iAnoLS6vve-KNV0q8TxKvMre7oi9IulDcqTuJ1alHrZAIVlgrgFn88MKirZuTqHG6LCtEsr7qGD9XyVcz64oXrb9vx4FO9tLNQxvdnIWCIwyPAYWtPMHMSSD5oEVUtVL_5IaxfCJvU-FchdHiwfxvXMWmA-i3mcEEe9zggag2vUPPIqUwbPVUFNj2hE7UsZbasuIToEMFRZqSB6juc9zv6PEUueQ5hAJCEylTkzMwyBMibrt04TmtZk2w9DfKJR91555s2ZMstX4G_su1_FqQ6p9vgcuLQ6tCtrW77tta-Rw7McF_tyPmvnhQ"
    )

ORCID_JWS_VERITY_ALGORITHM = "RS256"


class ClientCredentials(BaseModel):
    client_id: str
    client_secret: str


class TokenExpires(BaseModel):
    days: Optional[int] = 1
    hours: Optional[int] = 0
    minutes: Optional[int] = 0


ACCESS_TOKEN_EXPIRES = TokenExpires(days=0, hours=0, minutes=30)


class Token(BaseModel):
    access_token: str
    token_type: str
    expires: Optional[TokenExpires] = None


class TokenData(BaseModel):
    subject: Optional[str] = None


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

credentials_exception = HTTPException(
    status_code=HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_access_token_expiration(token) -> datetime:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("exp")
    except JWTError:
        raise credentials_exception


class OAuth2PasswordOrClientCredentialsBearer(OAuth2):
    def __init__(
        self,
        tokenUrl: str,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        auto_error: bool = True,
    ):
        if not scopes:
            scopes = {}
        flows = OAuthFlowsModel(
            password={"tokenUrl": tokenUrl, "scopes": scopes},
            clientCredentials={"tokenUrl": tokenUrl},
        )
        super().__init__(flows=flows, scheme_name=scheme_name, auto_error=auto_error)

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        scheme, param = get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() != "bearer":
            if self.auto_error:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="Not authenticated",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            else:
                print(request.url)
                return None
        return param


oauth2_scheme = OAuth2PasswordOrClientCredentialsBearer(
    tokenUrl="token", auto_error=False
)
optional_oauth2_scheme = OAuth2PasswordOrClientCredentialsBearer(
    tokenUrl="token", auto_error=False
)

bearer_scheme = HTTPBearer(scheme_name="bearerAuth", auto_error=False)


async def basic_credentials(req: Request):
    return await HTTPBasic(auto_error=False)(req)


async def bearer_credentials(req: Request):
    return await HTTPBearer(scheme_name="bearerAuth", auto_error=False)(req)


class OAuth2PasswordOrClientCredentialsRequestForm:
    def __init__(
        self,
        basic_creds: Optional[HTTPBasicCredentials] = Depends(basic_credentials),
        bearer_creds: Optional[HTTPAuthorizationCredentials] = Depends(
            bearer_credentials
        ),
        grant_type: str = Form(None, regex="^password$|^client_credentials$"),
        username: Optional[str] = Form(None),
        password: Optional[str] = Form(None),
        scope: str = Form(""),
        client_id: Optional[str] = Form(None),
        client_secret: Optional[str] = Form(None),
    ):
        if bearer_creds:
            self.grant_type = "client_credentials"
            self.username, self.password = None, None
            self.scopes = scope.split()
            self.client_id = bearer_creds.credentials
            self.client_secret = None
        elif grant_type == "password" and (username is None or password is None):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="grant_type password requires username and password",
            )
        elif grant_type == "client_credentials" and (client_id is None):
            if basic_creds:
                client_id = basic_creds.username
                client_secret = basic_creds.password
            else:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="grant_type client_credentials requires client_id and client_secret",
                )
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scopes = scope.split()
        self.client_id = client_id
        self.client_secret = client_secret
