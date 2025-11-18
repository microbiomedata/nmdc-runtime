from enum import Enum

from pydantic import BaseModel


class AllowanceAction(str, Enum):
    SUBMIT_CHANGESHEET = "/metadata/changesheets:submit"
    DELETE_DATA = "/queries:run(query_cmd:DeleteCommand)"
    AGGREGATE_DATA = "/queries:run(query_cmd:AggregateCommand)"
    SUBMIT_JSON = "/metadata/json:submit"
    WF_FILE_STAGING = "/wf_file_staging"
    MANAGE_ALLOWANCES = "/allowances"


class Allowance(BaseModel):
    """Model for an allowance record."""

    username: str
    action: AllowanceAction
