"""Module."""

import logging
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.database import Database as MongoDatabase
from starlette import status
from pymongo.errors import BulkWriteError
from fastjsonschema.exceptions import JsonSchemaValueException

from nmdc_runtime.api.db.mongo import get_async_mongo_db, get_mongo_db
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.util import nmdc_jsonschema_validate

router = APIRouter(
    prefix="/workflows/activities_test",
    tags=["workflow_execution_activities_test"]
)


@router.post("", status_code=status.HTTP_201_CREATED, response_model=list[int])
async def post_activity(
    data: dict,
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
    amdb: AsyncIOMotorDatabase = Depends(get_async_mongo_db),
) -> list[int]:
    """Post data to mongo

    Parameters
    ----------
    data :  nmdc compliant data
    """
    try:
        nmdc_jsonschema_validate(data)
        resp = []
        for col in data:
            logging.warning(col)
            # We can ignore duplicates for now
            try:
                res = await amdb[col.lower()].insert_many(data[col])
                ndocs = len(res.inserted_ids)
            except BulkWriteError as e:
                logging.error(str(e.details))
                for write_err in e.details["writeErrors"]:

                    if write_err['code'] != 11000:
                        logging.error(str(write_err))
                ndocs = e.details["nInserted"]
            resp.append(ndocs)
        logging.warning(str(resp))
        return resp
    except JsonSchemaValueException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=409, detail=e.details)
