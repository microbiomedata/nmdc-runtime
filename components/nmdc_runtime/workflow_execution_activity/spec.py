" " "Beans." ""
from abc import ABC, abstractmethod

from components.nmdc_runtime.workflow.spec import Workflow
from nmdc_schema.nmdc import (
    WorkflowExecutionActivity,
    Database,
)
from pydantic import BaseModel, ConfigDict


class ActivityTree(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    children: list["ActivityTree"] = []
    data: WorkflowExecutionActivity
    spec: Workflow


class ActivityQueriesABC(ABC):
    @abstractmethod
    async def create_activity(self, activity_set: Database) -> str:
        """Beans."""
        raise NotImplementedError

    @abstractmethod
    async def list_by_id(
        self, identifiers: list[str]
    ) -> list[WorkflowExecutionActivity]:
        """Beans."""
        raise NotImplementedError


class DataObjectQueriesABC(ABC):
    @abstractmethod
    async def create_data_object(self, data_object_set: Database) -> str:
        raise NotImplementedError
