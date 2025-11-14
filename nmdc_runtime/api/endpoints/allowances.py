from typing import List, Optional

from pymongo.database import Database
from fastapi import APIRouter, Depends, HTTPException, Query, status

from nmdc_runtime.api.endpoints.util import check_action_permitted
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.allowance import Allowance, AllowanceActions


router = APIRouter()

def check_can_run_allowances_endpoints(user: User):
    """
    Check if the user is permitted to run the allowances endpoints in this file.
    """
    if not check_action_permitted(user.username, "/allowances"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin users are allowed to issue /allowances commands.",
        )

@router.get("/allowances", response_model=List[Allowance])
def list_allowances(
    username: Optional[str] = Query(None, description="Filter by username"),
    action: Optional[AllowanceActions] = Query(None, description="Filter by action"),
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
    check_can_run_allowances_endpoints(user)
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
    "/allowances", response_model=Allowance, status_code=status.HTTP_201_CREATED
)
def create_allowance(
    allowance: Allowance,
    user: User = Depends(get_current_active_user),
    mdb: Database = Depends(get_mongo_db),
):
    """
    Create an allowance for a specific user and action.

    **Note:** This endpoint is only accessible to authenticated users.
    """
    check_can_run_allowances_endpoints(user)
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
    username: str = Query(..., description="Username for the allowance to delete"),
    action: AllowanceActions = Query(..., description="Action for the allowance to delete"),
    user: User = Depends(get_current_active_user),
    mdb: Database = Depends(get_mongo_db),
):
    """
    Delete an allowance for a specific user and action.
    """
    check_can_run_allowances_endpoints(user)
    # Attempt to delete the allowance
    result = mdb["_runtime.api.allow"].delete_one(
        {"username": username, "action": action}
    )

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No allowance found for user '{username}' and action '{action}'",
        )

    return {
        "result": "success",
        "detail": f"Allowance for user {username} and action {action} deleted successfully"
    }


@router.get("/allowances/actions", response_model=List[str])
def list_valid_actions(
    user: User = Depends(get_current_active_user),
):
    """
    Get all valid actions (distinct action values from the allowances collection).

    """
    check_can_run_allowances_endpoints(user)

    return sorted([action.value for action in AllowanceActions])
