import json

from fastapi import APIRouter, Depends
from pymongo.database import Database
from typing import Optional, Annotated

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.util import ListRequest, ListResponse
from nmdc_runtime.api.models.nmdc_schema import (
    DataObject,
    DataObjectListRequest,
    list_request_filter_to_mongo_filter,
)
from nmdc_runtime.api.models.site import (
    Site,
    maybe_get_current_client_site,
)
from nmdc_runtime.api.models.wfe_file_stages import Globus

router = APIRouter()

@router.get(
    "/globus", response_model=ListResponse[Globus], response_model_exclude_unset=True
)
def list_globus_records(
    req: Annotated[ListRequest, Query()],
    mdb: Database = Depends(get_mongo_db),
    maybe_site: Optional[Site] = Depends(maybe_get_current_client_site),
):
    pass

