import logging
from typing import List, Optional

from pymongo.database import Database
from fastapi import APIRouter, Depends, HTTPException, Query, status, Response

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import (
    check_action_permitted,
    strip_oid,
)
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.allowance import Allowance, AllowanceAction


router = APIRouter()


def check_can_manage_allowances(user: User):
    """
    Check if the user is permitted to run the allowances endpoints in this file.
    """
    if not check_action_permitted(
        user.username, AllowanceAction.MANAGE_ALLOWANCES.value
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin users are allowed to issue /allowances commands.",
        )


@router.get("/allowances")
def list_allowances(
    username: Optional[str] = Query(None, description="Filter allowances by username"),
    action: Optional[AllowanceAction] = Query(
        None, description="Filter allowances by action"
    ),
    user: User = Depends(get_current_active_user),
    mdb: Database = Depends(get_mongo_db),
):
    """
    Get list of allowances, optionally filtered by username and/or action.

    - If both username and action are provided, returns the specific allowance (if any).
    - If only username is provided, returns all allowances for that user.
    - If only action is provided, returns all allowances for that action.
    - If neither is provided, returns all allowances.

    """
    check_can_manage_allowances(user)
    filter_criteria = {}
    if username:
        filter_criteria["username"] = username
    if action:
        filter_criteria["action"] = action.value
    allowances = list(
        mdb["_runtime.api.allow"].find(
            filter=filter_criteria,
        )
    )
    rv = {}
    rv["resources"] = [strip_oid(d) for d in allowances]
    return rv


@router.post(
    "/allowances", response_model=Allowance, status_code=status.HTTP_201_CREATED
)
def create_allowance(
    allowance: Allowance,
    user: User = Depends(get_current_active_user),
    mdb: Database = Depends(get_mongo_db),
):
    """
    Create an allowance for a specific user and action.

    **Note:** This endpoint is only accessible to admins.
    """
    check_can_manage_allowances(user)
    # Create the allowance
    doc = {"username": allowance.username, "action": allowance.action}
    # Check if the allowance already exists
    existing = mdb["_runtime.api.allow"].find_one(doc)

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Allowance already exists for user '{allowance.username}' and action '{allowance.action}'",
        )

    try:
        mdb["_runtime.api.allow"].insert_one(doc)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create allowance: {str(e)}",
        )

    return Allowance(**doc)


@router.delete("/allowances", status_code=status.HTTP_204_NO_CONTENT)
def delete_allowance(
    username: str = Query(..., description="Username of the allowance to delete"),
    action: AllowanceAction = Query(
        ..., description="Action of the allowance to delete"
    ),
    user: User = Depends(get_current_active_user),
    mdb: Database = Depends(get_mongo_db),
):
    """
    Delete an allowance for a specific user and action.
    """
    check_can_manage_allowances(user)
    # Attempt to delete the allowance
    result = mdb["_runtime.api.allow"].delete_one(
        {"username": username, "action": action}
    )

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No allowance found for user '{username}' and action '{action}'",
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/allowances/actions", response_model=List[AllowanceAction])
def list_valid_actions(
    user: User = Depends(get_current_active_user),
):
    """
    Get all of the actions that can be in allowances
    """
    check_can_manage_allowances(user)

    return sorted([action.value for action in AllowanceAction])
