import re
from typing import List

import pymongo
from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError
from starlette import status
from toolz import assoc

from nmdc_runtime.api.core.idgen import (
    Base32Id,
    generate_id_unique,
    decode_id,
    encode_id,
)
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.id import IdRequest, Id

router = APIRouter()


@router.post("/ids", response_model=Id)
def create_id(
    id_req: IdRequest,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    # sping: "semi-opaque string" (https://n2t.net/e/n2t_apidoc.html).
    sping = generate_id_unique(mdb, ns=id_req.ns)
    shoulder = id_req.shoulder or "fk1"
    return {"id": f"{shoulder}-{sping}", "ns": id_req.ns}


pattern_shoulder = re.compile(r"[abcdefghjkmnpqrstvwxyz\-]+[0-9]")
pattern_shoulder_and_blade = re.compile(
    r"^(?P<shoulder>[abcdefghjkmnpqrstvwxyz\-]+[0-9])(?P<blade>[0-9abcdefghjkmnpqrstvwxyz\-]+$)"
)


def shoulder_and_blade(id_):
    m = re.match(pattern_shoulder_and_blade, id_)
    return m.group("shoulder"), m.group("blade")


@router.get("/ids/{id_}", response_model=Id)
def get_id(
    id_: str,
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    if re.match(pattern_shoulder, id_) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - invalid shoulder. Did you forget to include the shoulder?",
        )
    try:
        shoulder, blade = shoulder_and_blade(id_)
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
