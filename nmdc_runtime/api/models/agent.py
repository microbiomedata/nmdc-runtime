from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, StringConstraints

Orcid = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{4}-\d{4}-\d{4}")]
ClientID = str
ResourceExpression = str
Action = str


class AgentEnum(str, Enum):
    orcid = "orcid"
    client = "client"


class Privilege(BaseModel):
    re: ResourceExpression
    actions: list[Action]


class Role(BaseModel):
    id: str
    privileges: list[Privilege]


class UserRole(BaseModel):
    role: Role
    re: ResourceExpression


class Agent(BaseModel):
    id: Orcid | ClientID
    type: AgentEnum
    name: Optional[str]
    contact_email: Optional[str]
    roles: list[UserRole]
    privileges: list[Privilege]
    hashed_secret: Optional[str]
