from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Depends, Response, status

from nmdc_runtime.api.models.user import User, get_current_active_user

from .models.ingest import Ingest

from nmdc_runtime.api.endpoints.util import (
    persist_content_and_get_drs_object,
    _claim_job,
    _request_dagster_run,
    permitted,
    users_allowed,
)
from components.workflow.workflow.core import (
    DataObjectService,
    ReadsQCSequencingActivityService,
)

router = APIRouter(prefix="/outputs", tags=["outputs"])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=Dict[str, Any],
)
async def ingest(
    ingest: Ingest,
    user: User = Depends(get_current_active_user),
):
    try:
        drs_obj_doc = persist_content_and_get_drs_object(
            content=json.dumps(ingest.dict()),
            username=user.username,
            filename=None,
            content_type="application/json",
            description="JSON metadata in",
            id_ns="readsqc-in",
        )
        mgs_service = ReadsQCSequencingActivityService()
        data_object_service = DataObjectService()
        object_dict = [member.dict() for member in ingest.data_object_set]
        activity_dict = [
            member.dict() for member in ingest.reads_qc_analysis_activity_set
        ]
        object_result = [
            await data_object_service.create_data_object(data_object)
            for data_object in object_dict
        ]
        activity_result = [
            await mgs_service.create_mgs_activity(activity)
            for activity in activity_dict
        ]
        return drs_obj_doc

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500)
