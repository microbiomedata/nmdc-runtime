from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Depends, Response, status

from .models.ingest import Ingest

from components.workflow.workflow.core import (
    DataObjectService,
    get_data_object_service,
)

router = APIRouter(prefix="/outputs", tags=["outputs"])


@router.post(
    "", status_code=status.HTTP_201_CREATED, response_model=List[bool]
)
async def ingest(
    ingest: Ingest,
):
    try:
        data_object_service = DataObjectService()
        dictionaries = [member.dict() for member in ingest.data_object_set]
        result = [
            await data_object_service.create_data_object(data_object)
            for data_object in dictionaries
        ]
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500)
