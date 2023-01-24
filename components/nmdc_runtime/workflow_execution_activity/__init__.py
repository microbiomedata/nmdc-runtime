"""Provides methods for interacting with NMDC Workflow Execution Activities.

Workflow Execution Activies are a map of relevant data a user would like to have with
regards to job execution or instantiation within their local system.
"""
from .core import ActivityService, init_activity_service
from .spec import Database, WorkflowExecutionActivity
