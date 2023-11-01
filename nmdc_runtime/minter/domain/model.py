from enum import Enum
from typing import Optional

from pydantic import BaseModel, PositiveInt

from nmdc_runtime.minter.config import schema_classes


class Entity(BaseModel):
    """A domain object whose attributes may change but has a recognizable identity over time."""

    id: str | None = None


class ValueObject(BaseModel):
    """An immutable domain object whose attributes entirely define it.

    It is fungible with other identical objects.
    """


class Status(str, Enum):
    draft = "draft"
    registered = "registered"
    indexed = "indexed"


class MintingRequest(ValueObject):
    service: Entity
    requester: Entity
    schema_class: Entity
    how_many: PositiveInt


class AuthenticatedMintingRequest(ValueObject):
    schema_class: Entity = Entity(id=schema_classes()[0]["id"])
    how_many: PositiveInt = 1


class ResolutionRequest(ValueObject):
    service: Entity
    requester: Entity
    id_name: str


class BindingRequest(ResolutionRequest):
    metadata_record: dict


class AuthenticatedBindingRequest(ValueObject):
    id_name: str
    metadata_record: dict


class DeleteRequest(ResolutionRequest):
    pass


class AuthenticatedDeleteRequest(ValueObject):
    id_name: str


class Identifier(Entity):
    name: str
    typecode: Entity
    shoulder: Entity
    status: Status
    bindings: Optional[dict] = None


class Typecode(Entity):
    schema_class: str
    name: str
