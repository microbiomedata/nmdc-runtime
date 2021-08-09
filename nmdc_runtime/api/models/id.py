import re
from typing import Optional, Union

from pydantic import BaseModel, constr, PositiveInt

# NO i, l, o or u. Optional '-'s.
_scheme_and_naan = r"(?P<base>ark:[0-9]+)"
pattern_scheme_and_naan = re.compile(_scheme_and_naan)
_shoulder = r"(?P<shoulder>[abcdefghjkmnpqrstvwxyz]+[0-9])"
pattern_shoulder = re.compile(_shoulder)
_blade = r"(?P<blade>[0-9abcdefghjkmnpqrstvwxyz]+)"
pattern_blade = re.compile(_blade)
pattern_assigned_base_name = re.compile(_shoulder + _blade)

SchemeAndNaan = constr(regex=_scheme_and_naan)
Shoulder = constr(regex=_shoulder)
Blade = constr(regex=_blade)
AssignedBaseName = constr(regex=(_shoulder + _blade))
BaseObjectName = constr(regex=(_scheme_and_naan + "/" + _shoulder + _blade))

Minter = constr(regex=fr"(({_scheme_and_naan}/)|nmdc:){_shoulder}")


class MintRequest(BaseModel):
    populator: str = ""
    minter: Minter = "ark:99999/fk4"
    number: PositiveInt = 1


class IdThreeParts(BaseModel):
    scheme_and_naan: SchemeAndNaan
    shoulder: Shoulder
    blade = Blade


class IdTwoParts(BaseModel):
    scheme_and_naan: SchemeAndNaan
    assigned_base_name: AssignedBaseName


class IdWhole(BaseModel):
    base_object_name: BaseObjectName


class Id(BaseModel):
    id: Union[IdWhole, IdTwoParts, IdThreeParts]


class IdBindings(BaseModel):
    where: BaseObjectName
