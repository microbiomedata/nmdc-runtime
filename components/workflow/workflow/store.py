from datetime import datetime
from typing import Dict, List, Union

from beanie import Document, Indexed
from pydantic import DirectoryPath, HttpUrl, ValidationError

from .spec import (
    DataObject,
    MetaGenomeSequencingActivity,
    IDataObjectQueries,
    IMetaGenomeSequencingActivityQueries,
)


class DataObjectInDb(Document, DataObject):
    data_object_id: Indexed(str, unique=True)

    class Collection:
        name = "data_object_set"


class MetagenomeSequencingActivityInDb(Document, MetaGenomeSequencingActivity):
    activity_id: Indexed(str, unique=True)

    class Collection:
        name = "mgs_activity_set"


class DataObjectQueries(IDataObjectQueries):
    async def create_data_object(self, data_object: DataObject) -> bool:
        try:
            new_object = DataObjectInDb(**data_object.dict())
            result = await new_object.insert()
            return True
        except ValidationError as e:
            raise ValidationError from e

    async def by_id(self, id: str) -> DataObject:
        try:
            new_object = await DataObjectInDb.find_one(
                DataObjectInDb.data_object_id == id
            )
            return DataObjectInDb
        except ValidationError as e:
            raise ValidationError from e


class MetagenomeSequencingActivityQueries(
    IMetaGenomeSequencingActivityQueries
):
    async def create_activity(
        self, metagenome_sequencing_activity: MetaGenomeSequencingActivity
    ) -> bool:
        try:
            new_activity = MetagenomeSequencingActivityInDb(
                **metagenome_sequencing_activity.dict()
            )
            result = await new_activity.insert()
            return True
        except ValidationError as e:
            raise ValidationError from e

    async def by_id(self, id: str) -> MetaGenomeSequencingActivity:
        try:
            new_activity = await MetagenomeSequencingActivityInDb.find_one(
                MetagenomeSequencingActivityInDb.activity_id == id
            )
            return MetaGenomeSequencingActivity(**new_activity.dict())
        except ValidationError as e:
            raise ValidationError from e
