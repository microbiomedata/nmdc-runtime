from typing import Any, Dict

from .spec import DataObject, MetaGenomeSequencingActivity
from .store import (
    DataObjectInDb,
    DataObjectQueries,
    MetagenomeSequencingActivityInDb,
    MetagenomeSequencingActivityQueries,
)


class DataObjectService:
    """Service for handling nmdc data objects in nmdc runtime."""

    def __init__(
        self, data_object_queries: DataObjectQueries = DataObjectQueries()
    ) -> None:
        self.__queries = data_object_queries

    async def create_data_object(
        self, data_object: Dict[str, Any]
    ) -> Dict[str, Any]:
        """A function to create a new workflow job

        :param data_object: Dict[str, Any] dictionary of fields for data object creation

        :return stuff: Dict[str, Any] stuff
        """
        new_object = DataObject.parse_obj(data_object)
        result = await self.__queries.create_data_object(new_object)
        return result.dict()

    async def by_id(self, id: str) -> Dict[str, Any]:
        result = await self.__queries.by_id(id)
        return result.dict()


class MetagenomeSequencingActivityService:
    """Service for handling nmdc metagenome activities in nmdc runtime."""

    def __init__(
        self,
        activity_queries: MetagenomeSequencingActivityQueries = MetagenomeSequencingActivityQueries(),
    ) -> None:
        self.__queries = activity_queries

    async def create_mgs_activity(
        self, mgs_activity: Dict[str, Any]
    ) -> Dict[str, Any]:
        new_activity = MetaGenomeSequencingActivity.parse_obj(mgs_activity)
        return await self.__queries.create_activity(new_activity)

    async def by_id(self, id: str) -> Dict[str, Any]:
        result = await self.__queries.by_id(id)
        return result.dict()


def get_beanie_documents():
    return [DataObjectInDb, MetagenomeSequencingActivityInDb]


def get_data_object_service():
    return DataObjectService()
