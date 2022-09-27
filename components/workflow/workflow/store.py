from datetime import datetime
from typing import Dict, List, Union

from beanie import Document, Indexed
from pydantic import DirectoryPath, HttpUrl, ValidationError

from .spec import (
    DataObject,
    ReadsQCSequencingActivity,
    IDataObjectQueries,
    IReadsQCSequencingActivityQueries,
)


class DataObjectInDb(Document, DataObject):
    id: Indexed(str)

    # @classmethod
    # def create(cls, data_object: DataObject) -> "DataObject" :
    #     return await cls.in

    class Collection:
        name = "data_object_set"


class ReadsQCSequencingActivityInDb(Document, ReadsQCSequencingActivity):
    id: Indexed(str)

    class Collection:
        name = "reads_QC_analysis_activity_set"


class DataObjectQueries(IDataObjectQueries):
    async def create_data_object(self, data_object: DataObject) -> DataObject:
        try:
            new_object = DataObjectInDb(**data_object.dict())
            result = await new_object.insert()
            return DataObject(**result.dict())
        except ValidationError as e:
            raise ValidationError from e

    async def by_id(self, id: str) -> DataObject:
        try:
            new_object = await DataObjectInDb.find_one(DataObjectInDb.id == id)
            return DataObjectInDb
        except ValidationError as e:
            raise ValidationError from e


class ReadsQCSequencingActivityQueries(IReadsQCSequencingActivityQueries):
    async def create_activity(
        self, metagenome_sequencing_activity: ReadsQCSequencingActivity
    ) -> ReadsQCSequencingActivity:
        try:
            new_activity = ReadsQCSequencingActivityInDb(
                **metagenome_sequencing_activity.dict()
            )
            result = await new_activity.insert()
            return ReadsQCSequencingActivity(**result.dict())
        except ValidationError as e:
            raise ValidationError from e

    async def by_id(self, id: str) -> ReadsQCSequencingActivity:
        try:
            new_activity = await ReadsQCSequencingActivityInDb.find_one(
                ReadsQCSequencingActivityInDb.id == id
            )
            return ReadsQCSequencingActivity(**new_activity.dict())
        except ValidationError as e:
            raise ValidationError from e
