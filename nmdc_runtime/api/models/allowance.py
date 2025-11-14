from pydantic import BaseModel
from enum import Enum


class AllowanceActions(str, Enum):
    SUBMIT = "/metadata/changesheets:submit"
    DELETE = "/queries:run(query_cmd:DeleteCommand)"
    AGGREGATE = "/queries:run(query_cmd:AggregateCommand)"
    JSON_SUBMIT = "/metadata/json:submit"
    WF_FILE_STAGING = "/wf_file_staging"
    ALLOWANCES = "/allowances"


class Allowance(BaseModel):
    """Model for an allowance record."""

    username: str
    action: AllowanceActions
