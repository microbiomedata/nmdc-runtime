"""Module"""
from abc import ABC
from typing import Literal, Optional, Tuple, TypedDict, TypeVar

from pydantic import BaseModel, HttpUrl

T = TypeVar("T")

ResultWithErr = Tuple[T, Optional[Exception]]


class WorkflowModelBase(BaseModel):
    """
    Workflow mapping class.

    Fields:
    - `name` - Workflow document name.

    - `enabled` - boolean representing whether workflow is enabled or not

    - `git_repo` - Repository where workflow wdl is kept.

    - `version` - Version of the workflow being used

    - `wdl` - wdl file for workflow

    - `activity` - name of associated workflow activities

    - `predecessor` - workflow that is executed before this one.

    - `trigger` - the triggering data object
    - `input_prefix` - what prefix is used for inputs

    - `inputs` - map of input names to locations

    Inherited from:

    - Pydantic BaseModel
    """

    name: str
    enabled: bool
    git_repo: HttpUrl
    version: str
    wdl: str
    activity: str
    predecessor: list[str]
    trigger: str
    input_prefix: str
    inputs: dict[str, str]


class WorkflowModelReadQC(WorkflowModelBase):
    name: Literal["Read QC"]
    activity: Literal["nmdc:ReadQCAnalysisActivity"]
    trigger: Literal["Metagenome Raw Reads"]


class WorkflowModelMetagenomeAssembly(WorkflowModelBase):
    name: Literal["Read QC"]
    activity: Literal["nmdc:MetagenomeAssemblyActivity"]
    trigger: Literal["Filtered Sequencing Reads"]


class WorkflowModelMetagenomeAnnotation(WorkflowModelBase):
    name: Literal["Read QC"]
    activity: Literal["nmdc:MetagenomeAnnotationActivity"]
    trigger: Literal["Assembly Contigs"]


class WorkflowModelMAGs(WorkflowModelBase):
    name: Literal["MAGs"]
    activity: Literal["nmdc:MAGsActivity"]
    trigger: Literal["Functional Annotation GFF"]


class WorkflowModelReadbasedAnalysis(WorkflowModelBase):
    name: Literal["ReadbasedAnalysis"]
    activity: Literal["nmdc:ReadbasedAnalysisActivity"]
    trigger: Literal["Filtered Sequencing Reads"]


class Workflow(WorkflowModelBase):
    name: Literal[
        "Read QC",
        "Metagenome Assembly",
        "Metagenome Annotation",
        "MAGs",
        "Readbased Analysis",
    ]
    activity: Literal[
        "nmdc:ReadQCAnalysisActivity",
        "nmdc:MetagenomeAssemblyActivity",
        "nmdc:MetagenomeAnnotationActivity",
        "nmdc:MAGsActivity",
        "nmdc:ReadbasedAnalysisActivity",
    ]
    trigger: Literal[
        "Metagenome Raw Reads",
        "Filtered Sequencing Reads",
        "Assembly Contigs",
        "Functional Annotation GFF",
        "Filtered Sequencing Reads",
    ]


class WorkflowCreate(TypedDict):
    name: str
    enabled: bool
    git_repo: HttpUrl
    version: str
    wdl: str
    activity: str
    predecessor: Optional[str]
    trigger: str
    input_prefix: str
    inputs: dict[str, str]


class WorkflowUpdate(TypedDict, total=False):
    name: str
    enabled: bool
    git_repo: HttpUrl
    version: str
    wdl: str
    activity: str
    predecessor: Optional[str]
    trigger: str
    input_prefix: str
    inputs: dict[str, str]


class WorkflowQueriesABC(ABC):
    """Querying functions for workflows.

    Extend this class to create and wrap your queries whether they be to a
    database, file system, or any other kind of backend.
    """

    async def workflow_create(self, creation: WorkflowCreate) -> ResultWithErr[bool]:
        """Add a new workflow.

        Args:
            creation (WorkflowCreate): a typed dictionary containing initial data for a
        new workflow

        Returns:
            bool: a simple boolean indicating success or failure
        """
        raise NotImplementedError

    async def workflow_update(self, update: WorkflowUpdate) -> ResultWithErr[bool]:
        """Update an existing workflow.

        Args:
            update (WorkflowUpdate): a typed dictionary containing the fields to be
        updated

        Returns:
            bool: a simple boolean indicating success or failure
        """
        raise NotImplementedError
