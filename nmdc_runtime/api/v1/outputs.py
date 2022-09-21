from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Depends, Response, status

from .models.ingest import Ingest

from components.workflow.workflow.core import (
    DataObjectService,
    MetagenomeSequencingActivityService,
)

router = APIRouter(prefix="/outputs", tags=["outputs"])


@router.post(
    "", status_code=status.HTTP_201_CREATED, response_model=List[bool]
)
async def ingest(
    ingest: Ingest,
):
    try:
        mgs_service = MetagenomeSequencingActivityService()
        data_object_service = DataObjectService()
        object_dict = [member.dict() for member in ingest.data_object_set]
        activity_dict = [
            member.dict()
            for member in ingest.metagenome_sequencing_activity_set
        ]
        object_result = [
            await data_object_service.create_data_object(data_object)
            for data_object in object_dict
        ]
        activity_result = [
            await mgs_service.create_mgs_activity(activity)
            for activity in activity_dict
        ]
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500)
