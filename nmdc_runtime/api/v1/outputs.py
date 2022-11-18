from fastapi import APIRouter, Depends, HTTPException
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.site import Site, get_current_client_site
from pymongo import ReturnDocument
from pymongo.database import Database as MongoDatabase
from pymongo.errors import DuplicateKeyError
from starlette import status

from ..db.mongo import get_mongo_db
from ..models.object_type import DrsObjectWithTypes
from .models.ingest import Ingest

router = APIRouter(prefix="/outputs", tags=["outputs"])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=DrsObjectWithTypes,
)
async def ingest(
    ingest: Ingest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    try:

        if site is None:
            raise HTTPException(status_code=401, detail="Client site not found")

        drs_obj_doc = persist_content_and_get_drs_object(
            content=ingest.json(),
            filename=None,
            content_type="application/json",
            description="input metadata for readqc-in wf",
            id_ns="json-readqc-in",
        )

        doc_after = mdb.objects.find_one_and_update(
            {"id": drs_obj_doc["id"]},
            {"$set": {"types": ["readqc-in"]}},
            return_document=ReturnDocument.AFTER,
        )
        return doc_after

    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail=e.details)
