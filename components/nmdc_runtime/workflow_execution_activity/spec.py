"""Beans."""
from abc import ABC, abstractmethod
from pydantic.dataclasses import dataclass
from datetime import datetime
from typing import Literal, Type
from typing_extensions import TypedDict

from nmdc_schema.nmdc import (
    DataObject,
    DataObjectId,
    WorkflowExecutionActivity,
    WorkflowExecutionActivityId,
    Database,
)
from pydantic import BaseModel, HttpUrl
from typing_extensions import NotRequired


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
