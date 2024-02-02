from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, StringConstraints

Orcid = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{4}-\d{4}-\d{4}")]
ClientID = str
ResourceExpression = str


class Action(str, Enum):
    create = "create"
    read = "read"
    update = "update"
    delete = "delete"


class AuthMethod(str, Enum):
    orcid_jwt = "orcid_jwt"
    client_id_and_secret = "client_id_and_secret"


class Privilege(BaseModel):
    re: ResourceExpression
    actions: list[Action]


class Role(BaseModel):
    id: str
    privileges: list[Privilege]


class Agent(BaseModel):
    id: Orcid | ClientID
    auth_method: AuthMethod
    name: Optional[str]
    contact_email: Optional[str]
    roles: list[Role]
    privileges: list[Privilege]
    hashed_secret: Optional[str]
