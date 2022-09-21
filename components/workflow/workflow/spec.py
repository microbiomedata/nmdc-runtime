"""Workflow Object Specifications"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, HttpUrl, validator
from semver import Version


class DataObject(BaseModel):
    data_object_id: str
    name: str
    description: str
    file_size_bytes: int
    type: str
    data_object_type: str
    url: HttpUrl


class MetaGenomeSequencingActivity(BaseModel):
    has_input: List[str]
    part_of: List[str]
    git_url: Optional[HttpUrl]
    version: str
    has_output: List[str]
    was_informed_by: str
    activity_id: str
    name: str
    started_at_time: datetime
    ended_at_time: datetime
    type: str


class WorkflowInput(BaseModel):
    name: str
    location: HttpUrl


class WorkflowPredecessor(BaseModel):
    name: str
    version: str

    @validator("version")
    def version_to_string(cls, v):
        if not Version.isvalid(v):
            raise ValueError(f"{v} is not a semantic version")
        return str(v)


class Workflow(BaseModel):
    enabled: bool = False
    name: str
    description: str
    repository: HttpUrl
    wdl_repo: HttpUrl
    version: str
    trigger: str
    inputs: List[WorkflowInput]
    predecessor: Optional[WorkflowPredecessor]
    input_prefix: str
    created_at: datetime
    updated_at: datetime

    @validator("version")
    def version_to_string(cls, v):
        if not Version.isvalid(v):
            raise ValueError(f"{v} is not a semantic version")
        return str(v)

    class Config:
        orm_mode: True


class WorkflowUpdate(Workflow):
    verbose: bool = False
    description: Optional[str]
    wdl_repo: Optional[HttpUrl]
    trigger: Optional[str]
    inputs: Optional[List[WorkflowInput]]
    input_prefix: Optional[str]
    updated_at: datetime = datetime.now()


class WorkflowIn(Workflow):
    enabled: Optional[bool]
    verbose: bool = False
    wdl_repo: Optional[HttpUrl]
    trigger: Optional[str]
    inputs: Optional[List[WorkflowInput]]
    input_prefix: Optional[str]
    created_at: Optional[datetime]


class WorkflowOut(Workflow):
    wdl_repo: Optional[HttpUrl]
    trigger: Optional[str]
    inputs: Optional[List[WorkflowInput]]
    input_prefix: Optional[str]
    created_at: Optional[datetime]


class IMetaGenomeSequencingActivityQueries(ABC):
    """Query interface for metagenome activities"""

    @abstractmethod
    async def create_activity(
        metagenome_sequencing_activity: MetaGenomeSequencingActivity,
    ):
        pass

    @abstractmethod
    async def by_id(id: str) -> MetaGenomeSequencingActivity:
        pass


class IDataObjectQueries(ABC):
    """Query interface for interacting with data objects"""

    @abstractmethod
    async def create_data_object(self, data_object: DataObject):
        pass

    async def by_id(self, id: str) -> DataObject:
        pass


class IWorkflowQueries(ABC):
    """Query interface for interacting with workflows"""

    @abstractmethod
    async def create_workflow(workflow: Workflow) -> WorkflowOut:
        """Creates a workflow

        :param Workflow workflow: An object containing workflow information.
        """
        pass

    @abstractmethod
    async def update_workflow(workflow: WorkflowUpdate) -> WorkflowOut:
        """Updates a workflow if version has been updated.

        Requires the version in the update to be higher than the version in the
        existing record, otherwise this update will fail.

        :param WorkflowUpdate workflow: An object containing workflow update
        information.
        """
        pass

    @abstractmethod
    async def fetch_workflows(query) -> List[WorkflowOut]:
        pass

    @abstractmethod
    async def fetch_by_name(workflow_id: str, query) -> WorkflowOut:
        pass


class IMetagenomeSequencingActivityQueries(ABC):
    """Query interface for interacting with metagenome activies"""

    @abstractmethod
    async def create_activity(
        self, metagenome_sequencing_activity: MetaGenomeSequencingActivity
    ):
        pass

    @abstractmethod
    async def by_id(self, id: str) -> MetaGenomeSequencingActivity:
        pass
