import os
from datetime import datetime, timedelta
from typing import Optional, Dict

from fastapi import Depends
from fastapi.exceptions import HTTPException
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.param_functions import Form
from fastapi.security import OAuth2, HTTPBasic, HTTPBasicCredentials
from fastapi.security.utils import get_authorization_scheme_param
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from starlette.requests import Request
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"


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
    expires: Optional[TokenExpires]


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
                return None
        return param


oauth2_scheme = OAuth2PasswordOrClientCredentialsBearer(tokenUrl="token")


async def basic_credentials(req: Request):
    return await HTTPBasic(auto_error=False)(req)


class OAuth2PasswordOrClientCredentialsRequestForm:
    def __init__(
        self,
        basic_creds: Optional[HTTPBasicCredentials] = Depends(basic_credentials),
        grant_type: str = Form(None, regex="^password$|^client_credentials$"),
        username: Optional[str] = Form(None),
        password: Optional[str] = Form(None),
        scope: str = Form(""),
        client_id: Optional[str] = Form(None),
        client_secret: Optional[str] = Form(None),
    ):
        if grant_type == "password" and (username is None or password is None):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="grant_type password requires username and password",
            )
        if grant_type == "client_credentials" and (
            client_id is None or client_secret is None
        ):
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
