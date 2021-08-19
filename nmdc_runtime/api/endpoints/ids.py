import re
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import dissoc

from nmdc_runtime.api.core.idgen import (
    generate_ids,
    decode_id,
    collection_name,
)
from nmdc_runtime.api.core.util import raise404_if_none, pick
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.id import (
    MintRequest,
    pattern_shoulder,
    AssignedBaseName,
    pattern_assigned_base_name,
    IdBindingRequest,
    pattern_base_object_name,
    IdThreeParts,
    pattern_naa,
)
from nmdc_runtime.api.models.site import get_current_client_site, Site

router = APIRouter()


@router.post("/ids/mint", response_model=List[str])
def mint_ids(
    mint_req: MintRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """Generate one or more identifiers.

    Leaving `populator` blank will set it to the site ID of the request client.
    """
    ids = generate_ids(
        mdb,
        owner=site.id,
        populator=(mint_req.populator or site.id),
        number=mint_req.number,
        naa=mint_req.naa,
        shoulder=mint_req.shoulder,
    )
    return ids


@router.post("/ids/bindings", response_model=List[Dict[str, Any]])
def set_id_bindings(
    binding_requests: List[IdBindingRequest],
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    bons = [r.i for r in binding_requests]
    ids: List[IdThreeParts] = []
    for bon in bons:
        m = re.match(pattern_base_object_name, bon)
        ids.append(
            IdThreeParts(
                naa=m.group("naa"),
                shoulder=m.group("shoulder"),
                blade=m.group("blade"),
            )
        )
    # Ensure that user owns all supplied identifiers.
    for id_, r in zip(ids, binding_requests):
        collection = mdb.get_collection(collection_name(id_.naa, id_.shoulder))
        doc = collection.find_one({"_id": decode_id(str(id_.blade))}, ["__ao"])
        if doc is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"id {r.i} not found",
            )
        elif doc.get("__ao") != site.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"authenticated site client does not manage {r.i} "
                    f"(client represents site {site.id}).",
                ),
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
        collection = mdb.get_collection(collection_name(id_.naa, id_.shoulder))

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
    cleaned = rest.replace("-", "")
    parts = cleaned.split(":")
    if len(parts) != 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid ID - needs both name assigning authority (NAA) part"
                "(e.g. 'nmdc') and name part (e.g. 'fk4ra92'), separated by a colon (':')."
            ),
        )
    naa = parts[0]
    suffix_parts = parts[1].split("/")
    if len(suffix_parts) == 2 and suffix_parts[-1] != "":  # one '/', or ends with '/'
        assigned_base_name, attribute = suffix_parts
    else:
        assigned_base_name = suffix_parts[0]
        attribute = None

    if re.match(pattern_naa, naa) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid ID - invalid name assigning authority (NAA) '{naa}'.",
        )
    print(assigned_base_name)
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

    collection = mdb.get_collection(collection_name(naa, shoulder))
    d = raise404_if_none(collection.find_one({"_id": id_decoded}))
    d = dissoc(d, "_id")
    if attribute is not None:
        if attribute not in d:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"attribute '{attribute}' not found in "
                    f"{naa}:{assigned_base_name}."
                ),
            )
        rv = pick(["where", attribute], d)
    else:
        rv = d
    return rv
