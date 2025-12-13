from enum import Enum

from pydantic import BaseModel


class AllowanceAction(str, Enum):
    SUBMIT_CHANGESHEETS = "/metadata/changesheets:submit"
    DELETE_DATA = "/queries:run(query_cmd:DeleteCommand)"
    AGGREGATE_DATA = "/queries:run(query_cmd:AggregateCommand)"
    SUBMIT_JSON = "/metadata/json:submit"
    WF_FILE_STAGING = "/wf_file_staging"
    MANAGE_ALLOWANCES = "/admin/allowances"
    MANAGE_SITE_CLIENTS = "/admin/site_clients"


class Allowance(BaseModel):
    """
    A username-action pair used to indicate that a user can perform a specific action.
    """

    username: str
    action: AllowanceAction
