import re
from typing import List, Dict, Any

from pymongo.database import Database as MongoDatabase
from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError
from starlette import status
from toolz import dissoc

from nmdc_runtime.api.core.idgen import (
    generate_ids,
    decode_id,
)
from nmdc_runtime.api.core.util import raise404_if_none, pick
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.id import (
    MintRequest,
    pattern_scheme_and_naan,
    pattern_shoulder,
    AssignedBaseName,
    pattern_assigned_base_name,
    IdBindingRequest,
    pattern_base_object_name,
    IdThreeParts,
)
from nmdc_runtime.api.models.user import User, get_current_active_user

router = APIRouter()


@router.post("/ids/mint", response_model=List[str])
def mint_ids(
    mint_req: MintRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    naa = mint_req.naa
    if naa.startswith("ark:"):
        naan = naa.split(":", maxsplit=1)[1]
        if naan not in {"76954", "99999"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Invalid ARK NAAN. Accepting only 99999 (for testing) "
                    "and 76954 (for NMDC) at this time."
                ),
            )
    elif naa != "nmdc":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid name assigning authority (NAA). Accepting only 'nmdc' at this time.",
        )

    if not re.match(r"(fk|mga|mta|mba|mpa|oma)[0-9]", mint_req.shoulder):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid shoulder namespace. "
                "Valid beginning are {fk,mga,mta,mba,mpa,oma}."
            ),
        )

    ids = generate_ids(
        mdb,
        owner=user.username,
        populator=(mint_req.populator or user.username),
        number=mint_req.number,
        naa=mint_req.naa,
        shoulder=mint_req.shoulder,
    )
    return ids


@router.post("/ids/bindings", response_model=List[Dict[str, Any]])
def set_id_bindings(
    binding_requests: List[IdBindingRequest],
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    bons = [r.i for r in binding_requests]
    ids: List[IdThreeParts] = []
    for bon in bons:
        m = re.match(pattern_base_object_name, bon)
        ids.append(
            IdThreeParts(
                arklabel_and_naan=m.group("arklabel_and_naan"),
                shoulder=m.group("shoulder"),
                blade=m.group("blade"),
            )
        )
    # Ensure that user owns all supplied identifiers.
    for id_, r in zip(ids, binding_requests):
        coll_name = f'{id_.arklabel_and_naan.replace(":", "_")}_{id_.shoulder}'
        collection = mdb.get_collection(coll_name)
        doc = collection.find_one({"_id": decode_id(str(id_.blade))}, ["__ao"])
        if doc is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"id {r.i} not found",
            )
        elif doc.get("__ao") != user.username:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"authenticated user does not own {r.i}",
            )
    # Ensure no attempts to set reserved attributes.
    if any(r.a.startswith("__a") for r in binding_requests):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot set attribute names beginning with '__a'.",
        )
    # Process binding requests
    docs = []
    for id_, r in zip(ids, binding_requests):
        coll_name = f'{id_.arklabel_and_naan.replace(":", "_")}_{id_.shoulder}'
        collection = mdb.get_collection(coll_name)
        filter_ = {"_id": decode_id(id_.blade)}
        if r.o == "purge":
            docs.append(collection.find_one_and_delete(filter_))
        elif r.o == "rm":
            docs.append(collection.find_one_and_update(filter_, {"$unset": {r.a: ""}}))
        elif r.o == "set":
            docs.append(collection.find_one_and_update(filter_, {"$set": {r.a: r.v}}))
        elif r.o == "addToSet":
            docs.append(
                collection.find_one_and_update(filter_, {"$addToSet": {r.a: r.v}})
            )
        else:
            # Note: IdBindingRequest root_validator methods should preclude this.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid operation 'o'."
            )

        return [dissoc(d, "_id") for d in docs]


@router.get("/ids/bindings/{rest:path}", response_model=Dict[str, Any])
def get_id_bindings(
    rest: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    cleaned = rest.replace("nmdc:", "ark:76954/").replace("-", "")
    parts = cleaned.split("/")
    if len(parts) not in (2, 3):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid ID - needs both name assigning authority (NAA) part"
                "(e.g. 'nmdc:' or 'ark:99999/') and name part (e.g. 'fk4ra92')."
            ),
        )
    elif len(parts) == 2 or parts[-1] == "":  # one '/', or ends with '/'
        scheme_and_naan, assigned_base_name = parts[:2]
        attribute = None
    else:
        scheme_and_naan, assigned_base_name, attribute = parts

    if re.match(pattern_scheme_and_naan, scheme_and_naan) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - invalid base. Needs to be valid ARK base.",
        )
    if re.match(pattern_shoulder, assigned_base_name) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid ID - invalid shoulder. "
                "Every name part begins with a 'shoulder', a "
                "sequence of letters followed by a number, "
                "for example 'fk4'. "
                "Did you forget to include the shoulder?",
            ),
        )
    try:
        m = re.match(pattern_assigned_base_name, AssignedBaseName(assigned_base_name))
        shoulder, blade = m.group("shoulder"), m.group("blade")
        id_decoded = decode_id(blade)
    except (AttributeError, ValidationError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - characters used outside of base32.",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID - failed checksum. Did you copy it incorrectly?",
        )

    coll_name = f'{scheme_and_naan.replace(":", "_")}_{shoulder}'
    collection = mdb.get_collection(coll_name)
    d = raise404_if_none(collection.find_one({"_id": id_decoded}))
    d = dissoc(d, "_id")
    if attribute is not None:
        if attribute not in d:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"attribute '{attribute}' not found in "
                    f"{scheme_and_naan}/{assigned_base_name}."
                ),
            )
        rv = pick(["where", attribute], d)
    else:
        rv = d
    return rv
