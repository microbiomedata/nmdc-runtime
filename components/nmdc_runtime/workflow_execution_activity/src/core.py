from typing import Any, Dict

from beanie import Document

from .spec import ActivitySet, DataObject, WorkflowExecutionActivity
from .store import (
    ActivityQueries,
    DataObjectInDb,
    DataObjectQueries,
    WorkflowExecutionActivityInDb,
)


class DataObjectService:
    """Service for handling nmdc data objects in nmdc runtime."""

    def __init__(
        self, data_object_queries: DataObjectQueries = DataObjectQueries()
    ) -> None:
        self.__queries = data_object_queries

    async def create_data_object(self, data_object: DataObject) -> str:
        """
        A function to create a new workflow job.

        Parameters
        ----------
        data_object : DataObject
            dictionary of fields for data object creation

        Returns
        -------
        str
            DataObject identifier
        """
        return await self.__queries.create_data_object(data_object)


class ActivityService:
    """Repository for interacting with nmdc workflow execution activities."""

    def __init__(
        self,
        activity_queries: ActivityQueries = ActivityQueries(),
    ) -> None:
        """
        Workflow execution activity service.

        By default this class loads its query machinery using mongodb, but if a user wants to
        use a different db or otherwise they should inherit and overload the ActivityQueries
        methods.

        Parameters
        ----------
        activity_queries : ActivityQueries
            Queries for activity set collection."""
        self.__queries = activity_queries

    async def add_activity_set(
        self,
        activities: ActivitySet,
        data_object_service: DataObjectService = DataObjectService(),
    ) -> list[str]:
        """
        Store workflow activities.

        Parameters
        ----------
        activities : ActivitySet
            dictionary of fields for data object creation

        data_object_service : DataObjectService
            service for interacting with data objects

        Returns
        -------
        list[str]
            IDs for all activities added to the collection
        """
        _ = [
            await data_object_service.create_data_object(data_object)
            for data_object in activities.data_object_set
        ]
        return [
            await self.__queries.create_activity(activity)
            for activity in activities.activity_set
        ]


def init_activity_service() -> ActivityService:
    """
    Instantiates an activity service.

    Returns
    -------
    ActivityService
    """
    return ActivityService()


def init_beanie_documents() -> list:
    """
    Returns beanie classes for mongodb."""
    return [WorkflowExecutionActivityInDb, DataObjectInDb]
