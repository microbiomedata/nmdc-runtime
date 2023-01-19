"Beans"
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Annotated, Literal, Optional, TypedDict, Union

from pydantic import BaseModel, Field, HttpUrl
from typing_extensions import NotRequired


class WorkflowExecutionActivity(TypedDict):
    """Definition of workflow execution activities for 3rd party modules."""

    has_input: list[str]
    part_of: list[str]
    git_url: HttpUrl
    version: str
    has_output: list[str]
    input_read_bases: int
    was_informed_by: str
    was_associated_with: NotRequired[str]
    id: str
    name: str
    started_at_time: datetime
    ended_at_time: NotRequired[datetime]
    type: Literal[
        "nmdc:ReadQCAnalysisActivity",
        "nmdc:MetagenomeAssemblyActivity",
        "nmdc:MetagenomeAnnotationActivity",
        "nmdc:MAGsActivity",
        "nmdc:ReadbasedAnalysisActivity",
    ]
    execution_resource: str


class WorkflowExecutionActivityBase(BaseModel):
    """Represents an instance of an execution of a particular workflow."""

    has_input: list[str]
    part_of: list[str]
    git_url: HttpUrl
    version: str
    has_output: list[str]
    input_read_bases: int
    was_informed_by: str
    was_associated_with: Optional[str]
    id: str
    name: str
    started_at_time: datetime
    ended_at_time: datetime
    execution_resource: str


class ReadQCActivityModel(WorkflowExecutionActivityBase):
    type: Literal["nmdc:ReadQCAnalysisActivity"]
    output_read_count: int
    output_read_bases: int
    input_read_count: int
    input_read_bases: int


class MetagenomeSequencingAnalysisActivityModel(WorkflowExecutionActivityBase):
    type: Literal["nmdc:MetagenomeSequencingAnalysisActivity"]


WorkflowExecutionActivityModel = Annotated[
    Union[ReadQCActivityModel, MetagenomeSequencingAnalysisActivityModel],
    Field(discriminator="type"),
]


class DataObject(TypedDict, total=False):
    """An object that primarily consists of symbols that represent information.

    Files, records, and omics data are examples of data objects."""

    file_size_bytes: int
    md5_checksum: str
    data_object_type: str
    compression_type: NotRequired[str]
    was_generated_by: NotRequired[str]
    url: HttpUrl
    type: str
    name: str
    description: str
    id: str


class DataObjectModel(BaseModel):
    """An object that primarily consists of symbols that represent information.

    Files, records, and omics data are examples of data objects."""

    file_size_bytes: int
    md5_checksum: str
    data_object_type: str
    compression_type: Optional[str]
    was_generated_by: Optional[str]
    url: HttpUrl
    type: str
    name: str
    description: str
    id: str


class ActivitySet(BaseModel):
    """More thought."""

    activity_set: list[WorkflowExecutionActivity]
    data_object_set: list[DataObject]


class ActivityQueriesABC(ABC):
    @abstractmethod
    async def create_activity(self, activity: WorkflowExecutionActivity) -> str:
        """Beans"""
        raise NotImplementedError

    @abstractmethod
    async def list_by_id(
        self, identifiers: list[str]
    ) -> list[WorkflowExecutionActivity]:
        """Beans"""
        raise NotImplementedError


class DataObjectQueriesABC(ABC):
    @abstractmethod
    async def create_data_object(self, data_object: DataObject) -> str:
        raise NotImplementedError
