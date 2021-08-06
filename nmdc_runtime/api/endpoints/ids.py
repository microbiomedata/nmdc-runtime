import re
from typing import List

import pymongo
from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError
from starlette import status
from toolz import assoc

from nmdc_runtime.api.core.idgen import (
    Base32Id,
    generate_ids,
    decode_id,
    encode_id,
)
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.id import MintRequest, Id
from nmdc_runtime.api.models.user import User, get_current_active_user

router = APIRouter()


@router.post("/ids/mint", response_model=List[str])
def mint_ids(
    mint_req: MintRequest,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    minter = mint_req.minter
    if minter.startswith("ark:"):
        naan = minter.split(":", maxsplit=1)[1].split("/", maxsplit=1)[0]
        if naan not in {"76954", "99999"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Invalid ARK NAAN. Accepting only 99999 (for testing) "
                    "and 76954 (for NMDC) at this time."
                ),
            )
    ids = generate_ids(
        mdb,
        populator=(mint_req.populator or user.username),
        number=mint_req.number,
        minter=mint_req.minter,
    )
    return ids


pattern_base = re.compile(r"ark:[0-9]+/")
pattern_shoulder = re.compile(r"[abcdefghjkmnpqrstvwxyz\-]+[0-9]")
pattern_shoulder_blade = re.compile(
    r"[abcdefghjkmnpqrstvwxyz\-]+[0-9][0-9abcdefghjkmnpqrstvwxyz]+"
)
pattern_base_shoulder_blade = re.compile(
    r"^(?P<base>ark:[0-9]+/)"
    r"(?P<shoulder>[abcdefghjkmnpqrstvwxyz\-]+[0-9])"
    r"(?P<blade>[0-9abcdefghjkmnpqrstvwxyz\-]+)"
)


def base_shoulder_blade(s):
    m = re.match(pattern_base_shoulder_blade, s)
    return (
        m.group("base"),
        m.group("shoulder"),
        m.group("blade"),
    )


@router.get("/ids/bindings/{rest:path}", response_model=Id)
def get_id_bindings(
    rest: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    cleaned = rest.replace("nmdc:", "ark:76954/")
    before, _, after = cleaned.rpartition("/")
    if re.match(pattern_shoulder_blade, after):
        base, shoulder, blade = base_shoulder_blade(cleaned)
        attribute = None
    elif re.match(pattern_base, cleaned) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - invalid base. Needs to be valid ARK base.",
        )
    elif re.match(pattern_shoulder, cleaned.split("/", maxsplit=1)[1]) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - invalid shoulder. Did you forget to include the shoulder?",
        )
    else:
        base, shoulder, blade = base_shoulder_blade(before)
        attribute = after

    # TODO lots

    try:
        id_decoded = decode_id(Base32Id(blade))
    except (AttributeError, ValidationError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - characters used outside of base32.",
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - failed checksum. Did you copy it incorrectly?",
        )
    d = raise404_if_none(mdb.ids.find_one({"_id": id_decoded}))
    return assoc(d, "id", f'{shoulder}-{encode_id(d["_id"])}')
