import re
from enum import Enum
from typing import Union, Any, Optional, Literal

from pydantic import BaseModel, constr, PositiveInt, root_validator

# NO i, l, o or u.
# ref: https://www.crockford.com/base32.html
base32_letters = "abcdefghjkmnpqrstvwxyz"

# Archival Resource Key (ARK) identifier scheme
# ref: https://www.ietf.org/archive/id/draft-kunze-ark-35.html
#
# NAA - Name Assigning Authority
NAA_VALUES = ["nmdc"]

# The base compact name assigned by the NAA consists of
# (a) a "shoulder", and (b) a final string known as the "blade".
# (The shoulder plus blade terminology mirrors locksmith jargon describing
# the information-bearing parts of a key.)
#
# Shoulders may reserved for internal departments or units.
# In the case of one central minting service, there technically need only be one shoulder.
# ref: https://www.ietf.org/archive/id/draft-kunze-ark-35.html#name-optional-shoulders
#
# For NMDC, semantically meaningful typecodes are desired for IDs.
# Solution described at <https://gist.github.com/dwinston/083a1cb508bbff21d055e7613f3ac02f>.
# In essence, bridging is needed between (a) the now-legacy shoulders and identifier structure
# `nmdc:<shoulder><generated_id>`and (b) the desired structure
# `nmdc:<type_code><shoulder><generated_id>`.
# The difference in shoulder structure is that legacy shoulders are of the pattern r"[a-z]+[0-9]",
# whereas current shoulders are to be of the pattern r"[0-9][a-z]*[0-9]" so that, in concert with
# the requirement that typcodes be of the pattern r"[a-z]{1,6}", a processor can identify (optional)
# typecode and subsequent shoulder syntactically.
#
# legacy shoulders
LEGACY_FAKE_SHOULDERS = [f"fk{n}" for n in range(10)]
LEGACY_ALLOCATED_SHOULDERS = ["mga0", "mta0", "mba0", "mpa0", "oma0"]
LEGACY_SHOULDER_VALUES = LEGACY_FAKE_SHOULDERS + LEGACY_ALLOCATED_SHOULDERS

TYPECODES = {
    "nmdc:Sample": ["sa"],
    "nmdc:Study": ["st"],
}

FAKE_SHOULDERS = [f"{n}fk{n}" for n in range(10)]
ALLOCATED_SHOULDERS = ["11"]
SHOULDER_VALUES = FAKE_SHOULDERS + ALLOCATED_SHOULDERS

_naa = rf"(?P<naa>({'|'.join(NAA_VALUES)}))"
pattern_naa = re.compile(_naa)
_typecode = rf"(?P<typecode>({'|'.join(SHOULDER_VALUES)}))"
_shoulder = rf"(?P<shoulder>({'|'.join(SHOULDER_VALUES)}))"
pattern_shoulder = re.compile(_shoulder)
_blade = rf"(?P<blade>[0-9{base32_letters}]+)"
pattern_blade = re.compile(_blade)
_assigned_base_name = f"{_shoulder}{_blade}"
pattern_assigned_base_name = re.compile(_assigned_base_name)
_base_object_name = f"{_naa}:{_shoulder}{_blade}"
pattern_base_object_name = re.compile(_base_object_name)

Naa = constr(regex=_naa)
Shoulder = constr(regex=rf"^{_shoulder}$", min_length=2)
Blade = constr(regex=_blade, min_length=4)
AssignedBaseName = constr(regex=_assigned_base_name)
BaseObjectName = constr(regex=_base_object_name)

NameAssigningAuthority = Literal[tuple(NAA_VALUES)]


class MintRequest(BaseModel):
    populator: str = ""
    naa: NameAssigningAuthority = "nmdc"
    shoulder: Shoulder = "fk0"
    number: PositiveInt = 1


class IdThreeParts(BaseModel):
    naa: Naa
    shoulder: Shoulder
    blade: Blade


class IdTwoParts(BaseModel):
    naa: Naa
    assigned_base_name: AssignedBaseName


class IdWhole(BaseModel):
    base_object_name: BaseObjectName


class Id(BaseModel):
    id: Union[IdWhole, IdTwoParts, IdThreeParts]


class IdBindings(BaseModel):
    where: BaseObjectName


class IdBindingOp(str, Enum):
    set = "set"
    addToSet = "addToSet"
    rm = "rm"
    purge = "purge"


class IdBindingRequest(BaseModel):
    i: BaseObjectName
    o: IdBindingOp = IdBindingOp.set
    a: Optional[str]
    v: Any

    @root_validator()
    def set_or_add_needs_value(cls, values):
        op = values.get("o")
        if op in (IdBindingOp.set, IdBindingOp.addToSet):
            if "v" not in values:
                raise ValueError("{'set','add'} operations needs value 'v'.")
        return values

    @root_validator()
    def set_or_add_or_rm_needs_attribute(cls, values):
        op = values.get("o")
        if op in (IdBindingOp.set, IdBindingOp.addToSet, IdBindingOp.rm):
            if not values.get("a"):
                raise ValueError("{'set','add','rm'} operations need attribute 'a'.")
        return values
