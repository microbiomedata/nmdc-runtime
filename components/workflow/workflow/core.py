from typing import Any, Dict

from .spec import DataObject, MetaGenomeSequencingActivity
from .store import DataObjectInDb, DataObjectQueries


def get_beanie_document():
    return [DataObjectInDb]


class DataObjectService:
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
        return await self.__queries.create_data_object(new_object)

    async def by_id(self, id: str) -> Dict[str, Any]:
        result = await self.__queries.by_id(str)
        return result.dict()


def get_data_object_service():
    return DataObjectService()
