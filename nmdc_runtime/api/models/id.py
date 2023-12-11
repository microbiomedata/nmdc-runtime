import re
from enum import Enum
from typing import Union, Any, Optional, Literal

from pydantic import model_validator, StringConstraints, BaseModel, PositiveInt
from typing_extensions import Annotated

# NO i, l, o or u.
base32_letters = "abcdefghjkmnpqrstvwxyz"

NAA_VALUES = ["nmdc"]
_fake_shoulders = [f"fk{n}" for n in range(10)]
SHOULDER_VALUES = _fake_shoulders + ["mga0", "mta0", "mba0", "mpa0", "oma0"]

_naa = rf"(?P<naa>({'|'.join(NAA_VALUES)}))"
pattern_naa = re.compile(_naa)
_shoulder = rf"(?P<shoulder>({'|'.join(SHOULDER_VALUES)}))"
pattern_shoulder = re.compile(_shoulder)
_blade = rf"(?P<blade>[0-9{base32_letters}]+)"
pattern_blade = re.compile(_blade)
_assigned_base_name = f"{_shoulder}{_blade}"
pattern_assigned_base_name = re.compile(_assigned_base_name)
_base_object_name = f"{_naa}:{_shoulder}{_blade}"
pattern_base_object_name = re.compile(_base_object_name)

Naa = Annotated[str, StringConstraints(pattern=_naa)]
Shoulder = Annotated[str, StringConstraints(pattern=rf"^{_shoulder}$", min_length=2)]
Blade = Annotated[str, StringConstraints(pattern=_blade, min_length=4)]
AssignedBaseName = Annotated[str, StringConstraints(pattern=_assigned_base_name)]
BaseObjectName = Annotated[str, StringConstraints(pattern=_base_object_name)]

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
    a: Optional[str] = None
    v: Any = None

    @model_validator(mode="before")
    def set_or_add_needs_value(cls, values):
        op = values.get("o")
        if op in (IdBindingOp.set, IdBindingOp.addToSet):
            if "v" not in values:
                raise ValueError("{'set','add'} operations needs value 'v'.")
        return values

    @model_validator(mode="before")
    def set_or_add_or_rm_needs_attribute(cls, values):
        op = values.get("o")
        if op in (IdBindingOp.set, IdBindingOp.addToSet, IdBindingOp.rm):
            if not values.get("a"):
                raise ValueError("{'set','add','rm'} operations need attribute 'a'.")
        return values
