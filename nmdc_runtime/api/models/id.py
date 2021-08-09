import re
from enum import Enum
from typing import Union, Any, Optional

from pydantic import BaseModel, constr, PositiveInt, root_validator

# NO i, l, o or u. Optional '-'s.
_arklabel_and_naan = r"(?P<arklabel_and_naan>ark:[0-9]+)"
pattern_scheme_and_naan = re.compile(_arklabel_and_naan)
_shoulder = r"(?P<shoulder>[abcdefghjkmnpqrstvwxyz]+[0-9])"
pattern_shoulder = re.compile(_shoulder)
_blade = r"(?P<blade>[0-9abcdefghjkmnpqrstvwxyz]+)"
pattern_blade = re.compile(_blade)
pattern_assigned_base_name = re.compile(_shoulder + _blade)
pattern_base_object_name = re.compile(_arklabel_and_naan + "/" + _shoulder + _blade)

ArkLabelAndNaan = constr(regex=_arklabel_and_naan)
Shoulder = constr(regex=_shoulder, min_length=2)
Blade = constr(regex=_blade, min_length=4)
AssignedBaseName = constr(regex=(_shoulder + _blade))
BaseObjectName = constr(regex=(_arklabel_and_naan + "/" + _shoulder + _blade))

NameAssigningAuthority = constr(regex=fr"({_arklabel_and_naan}|nmdc)")


class MintRequest(BaseModel):
    populator: str = ""
    naa: NameAssigningAuthority = "ark:76954"
    shoulder: Shoulder = "fk4"
    number: PositiveInt = 1


class IdThreeParts(BaseModel):
    arklabel_and_naan: ArkLabelAndNaan
    shoulder: Shoulder
    blade: Blade


class IdTwoParts(BaseModel):
    arklabel_and_naan: ArkLabelAndNaan
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
