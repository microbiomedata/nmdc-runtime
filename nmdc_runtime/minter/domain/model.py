from enum import Enum
import re
from typing import Optional

from base32_lib import base32
from pydantic import BaseModel, PositiveInt

from nmdc_runtime.minter.config import schema_classes, typecodes


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


id_prefix_pattern = rf"(?P<prefix>nmdc)"
id_typecode_pattern = rf"(?P<typecode>[a-z]{{1,6}})"
id_shoulder_pattern = rf"(?P<shoulder>[0-9][a-z]{{0,6}}[0-9])"
id_blade_pattern = rf"(?P<blade>[A-Za-z0-9]+)"
id_version_pattern = rf"(?P<version>(\.[A-Za-z0-9]+)*)"
id_locus_pattern = rf"(?P<locus>_[A-Za-z0-9_\.-]+)?"
id_pattern = (
    rf"^{id_prefix_pattern}:{id_typecode_pattern}-{id_shoulder_pattern}-"
    rf"{id_blade_pattern}{id_version_pattern}{id_locus_pattern}$"
)
ID_TYPECODE_VALUES = [t["name"] for t in typecodes()]
id_typecode_pattern_strict = rf"(?P<typecode_strict>({'|'.join(ID_TYPECODE_VALUES)}))"
id_blade_pattern_strict = rf"(?P<blade_strict>[{base32.ENCODING_CHARS}]+)"
id_pattern_strict = (
    rf"^{id_prefix_pattern}:{id_typecode_pattern_strict}-{id_shoulder_pattern}-"
    rf"{id_blade_pattern_strict}{id_version_pattern}{id_locus_pattern}$"
)
id_pattern_strict_compiled = re.compile(id_pattern_strict)


def check_valid_ids(ids: list[str]):
    for id_ in ids:
        if not re.match(id_pattern, id_):
            raise ValueError(
                (
                    f"Invalid ID format for given ID: '{id_}'.\n\nAn ID must match the pattern: '{id_pattern}'.\n\n"
                    "See: <https://microbiomedata.github.io/nmdc-schema/identifiers/#ids-minted-for-use-within-nmdc>"
                )
            )
    return ids
