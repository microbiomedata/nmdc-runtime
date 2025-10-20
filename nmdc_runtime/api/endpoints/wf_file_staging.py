from fastapi import HTTPException, status

from nmdc_runtime.api.models.user import User
from nmdc_runtime.api.endpoints.util import check_action_permitted


def check_can_run_wf_file_staging_endpoints(user: User):
    """
    Check if the user is permitted to run the wf_file_staging endpoints in this file.
    """
    if not check_action_permitted(user.username, "/wf_file_staging:run"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to issue wf_file_staging commands.",
        )
