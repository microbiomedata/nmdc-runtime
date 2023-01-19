from datetime import datetime
from typing import Dict, List, Literal, TypedDict, Union, cast

from beanie import Document, Indexed, Link
from beanie.operators import In
from pydantic import ValidationError
from pymongo.errors import DuplicateKeyError

from .spec import (ActivityQueriesABC, DataObject, DataObjectModel,
                   DataObjectQueriesABC, WorkflowExecutionActivity,
                   WorkflowExecutionActivityModel)


class DataObjectInDb(Document, DataObjectModel):
    id: Indexed(str, unique=True)  # type: ignore
    md5_checksum: Indexed(str, unique=True)  # type: ignore

    class Collection:
        """Describe Collection"""

        name = "data_objects"


class WorkflowExecutionActivityInDb(Document):
    activity: WorkflowExecutionActivityModel
    type: Literal[
        "nmdc:ReadQCAnalysisActivity",
        "nmdc:MetagenomeAssemblyActivity",
        "nmdc:MetagenomeAnnotationActivity",
        "nmdc:MAGsActivity",
        "nmdc:ReadbasedAnalysisActivity",
    ]
    id: Indexed(str, unique=True)  # type: ignore
    data_objects: list[Link[DataObjectInDb]]

    class Collection:
        name = "workflow_execution_activities"


class ActivityQueries(ActivityQueriesABC):
    async def create_activity(self, activity: WorkflowExecutionActivity) -> str:
        """Description"""
        try:
            new_activity: WorkflowExecutionActivityInDb = (
                WorkflowExecutionActivityInDb.parse_obj(activity)
            )
            await new_activity.insert()
            return new_activity.id
        except DuplicateKeyError as error:
            raise error
        except ValidationError as error:
            raise error

    async def list_by_id(
        self, identifiers: list[str]
    ) -> list[WorkflowExecutionActivity]:
        """Beans"""
        try:
            activities: list[
                WorkflowExecutionActivityInDb
            ] = await WorkflowExecutionActivityInDb.find(
                In(WorkflowExecutionActivityInDb.id, identifiers)
            ).to_list()
            return [
                cast(WorkflowExecutionActivity, activity.dict())
                for activity in activities
            ]
        except ValidationError as error:
            raise error


class DataObjectQueries(DataObjectQueriesABC):
    async def create_data_object(self, data_object: DataObject) -> str:
        """Beans"""
        try:
            new_object = DataObjectInDb(**data_object)
            await new_object.insert()
            return new_object.id
        except DuplicateKeyError as error:
            raise error
        except ValidationError as error:
            raise error
