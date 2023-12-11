import traceback

from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database as MongoDatabase
from starlette import status

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.site import get_current_client_site, Site
from nmdc_runtime.minter.adapters.repository import MongoIDStore, MinterError
from nmdc_runtime.minter.config import minting_service_id, schema_classes
from nmdc_runtime.minter.domain.model import (
    Identifier,
    AuthenticatedMintingRequest,
    MintingRequest,
    Entity,
    ResolutionRequest,
    BindingRequest,
    AuthenticatedBindingRequest,
    AuthenticatedDeleteRequest,
    DeleteRequest,
)

router = APIRouter()

service = Entity(id=minting_service_id())


@router.post("/mint")
def mint_ids(
    req_mint: AuthenticatedMintingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> list[str]:
    """Mint one or more (typed) persistent identifiers."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        minted = s.mint(
            MintingRequest(
                service=service,
                requester=requester,
                **req_mint.model_dump(),
            )
        )
        return [d.id for d in minted]
    except MinterError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=traceback.format_exc(),
        )


@router.get("/resolve/{id_name}")
def resolve_id(
    id_name: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> Identifier:
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    id_ = s.resolve(
        ResolutionRequest(service=service, requester=requester, id_name=id_name)
    )
    return raise404_if_none(id_)


@router.post("/bind")
def bind(
    req_bind: AuthenticatedBindingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> Identifier:
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        id_ = s.bind(
            BindingRequest(
                service=service,
                requester=requester,
                id_name=req_bind.id_name,
                metadata_record=req_bind.metadata_record,
            )
        )
        return raise404_if_none(id_)
    except Exception as e:
        if str(e) == f"ID {req_bind.id_name} is unknown":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=traceback.format_exc(),
            )


@router.post("/delete")
def delete(
    req_del: AuthenticatedDeleteRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        id_ = s.delete(
            DeleteRequest(
                service=service,
                requester=requester,
                id_name=req_del.id_name,
            )
        )
    except Exception as e:
        if str(e) == f"ID {req_del.id_name} is unknown":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        elif str(e) == "Status not 'draft'. Can't delete.":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=traceback.format_exc(),
            )
