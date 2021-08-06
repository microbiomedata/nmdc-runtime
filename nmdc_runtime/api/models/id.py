from typing import Optional

from pydantic import BaseModel, constr, PositiveInt

from nmdc_runtime.api.core.idgen import Base32Id


Minter = constr(regex=r"^((ark:[0-9]+/)|nmdc:)[abcdefghjkmnpqrstvwxyz]+[0-9]$")


class MintRequest(BaseModel):
    populator: str = ""
    minter: Minter = "ark:99999/fk4"
    number: PositiveInt = 1


class Id(BaseModel):
    id: Base32Id
    ns: Optional[str]
