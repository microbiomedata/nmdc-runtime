"""Beans."""

from typing import List

from nmdc_runtime.workflow_execution_activity import (
    DataObject,
    WorkflowExecutionActivity,
    init_activity_service,
)
from pydantic import BaseModel


class ActivitySet(BaseModel):
    """More thought."""

    activity_set: List[WorkflowExecutionActivity]
    data_object_set: List[DataObject]
