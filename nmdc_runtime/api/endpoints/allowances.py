from typing import List, Optional

import pymongo.database
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.user import User, get_current_active_user


router = APIRouter()


class Allowance(BaseModel):
    """Model for an allowance record."""

    username: str
    action: str


class AllowanceCreate(BaseModel):
    """Model for creating an allowance."""

    username: str
    action: str


@router.get("/admin/allowances", response_model=List[Allowance])
def list_allowances(
    username: Optional[str] = Query(None, description="Filter by username"),
    action: Optional[str] = Query(None, description="Filter by action"),
    user: User = Depends(get_current_active_user),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Get list of allowances, optionally filtered by username and/or action.

    - If both username and action are provided, returns the specific allowance (if any).
    - If only username is provided, returns all allowances for that user.
    - If only action is provided, returns all allowances for that action.
    - If neither is provided, returns all allowances.

    **Note:** This endpoint is only accessible to authenticated users.
    """
    # Build the filter based on provided query parameters
    filter_dict = {}
    if username is not None:
        filter_dict["username"] = username
    if action is not None:
        filter_dict["action"] = action

    # Query the database
    allowances = list(mdb["_runtime.api.allow"].find(filter_dict, {"_id": 0}))

    return allowances


@router.post(
    "/admin/allowances", response_model=Allowance, status_code=status.HTTP_201_CREATED
)
def create_allowance(
    allowance: AllowanceCreate,
    user: User = Depends(get_current_active_user),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Create an allowance for a specific user and action.

    **Note:** This endpoint is only accessible to authenticated users.
    """
    # Check if the allowance already exists
    existing = mdb["_runtime.api.allow"].find_one(
        {"username": allowance.username, "action": allowance.action}
    )

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Allowance already exists for user '{allowance.username}' and action '{allowance.action}'",
        )

    # Create the allowance
    doc = {"username": allowance.username, "action": allowance.action}
    mdb["_runtime.api.allow"].insert_one(doc)

    return Allowance(**doc)


@router.delete("/admin/allowances", status_code=status.HTTP_204_NO_CONTENT)
def delete_allowance(
    username: str = Query(..., description="Username for the allowance to delete"),
    action: str = Query(..., description="Action for the allowance to delete"),
    user: User = Depends(get_current_active_user),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Delete an allowance for a specific user and action.

    **Note:** This endpoint is only accessible to authenticated users.
    """
    # Attempt to delete the allowance
    result = mdb["_runtime.api.allow"].delete_one(
        {"username": username, "action": action}
    )

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No allowance found for user '{username}' and action '{action}'",
        )

    return None


@router.get("/admin/allowances/actions", response_model=List[str])
def list_valid_actions(
    user: User = Depends(get_current_active_user),
    mdb: pymongo.database.Database = Depends(get_mongo_db),
):
    """
    Get all valid actions (distinct action values from the allowances collection).

    **Note:** This endpoint is only accessible to authenticated users.
    """
    # Get distinct action values from the allowances collection
    actions = mdb["_runtime.api.allow"].distinct("action")

    return sorted(actions)
