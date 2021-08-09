import re
from typing import Union

from pydantic import BaseModel, constr, PositiveInt

# NO i, l, o or u. Optional '-'s.
_arklabel_and_naan = r"(?P<base>ark:[0-9]+)"
pattern_scheme_and_naan = re.compile(_arklabel_and_naan)
_shoulder = r"(?P<shoulder>[abcdefghjkmnpqrstvwxyz]+[0-9])"
pattern_shoulder = re.compile(_shoulder)
_blade = r"(?P<blade>[0-9abcdefghjkmnpqrstvwxyz]+)"
pattern_blade = re.compile(_blade)
pattern_assigned_base_name = re.compile(_shoulder + _blade)

ArkLabelAndNaan = constr(regex=_arklabel_and_naan)
Shoulder = constr(regex=_shoulder)
Blade = constr(regex=_blade)
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
    blade = Blade


class IdTwoParts(BaseModel):
    arklabel_and_naan: ArkLabelAndNaan
    assigned_base_name: AssignedBaseName


class IdWhole(BaseModel):
    base_object_name: BaseObjectName


class Id(BaseModel):
    id: Union[IdWhole, IdTwoParts, IdThreeParts]


class IdBindings(BaseModel):
    where: BaseObjectName
