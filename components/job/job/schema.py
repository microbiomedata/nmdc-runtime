"""Description of job objects
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List
from datetime import datetime

from pydantic import BaseModel, DirectoryPath, HttpUrl, validator
from semver import Version


class PydanticVersion(Version):
    @classmethod
    def __get_validators__(cls):
        """Return a list of validator methods for pydantic models."""
        yield cls.parse

    @classmethod
    def __modify_schema__(cls, field_schema):
        """Inject/mutate the pydantic field schema in-place."""
        field_schema.update(
            examples=[
                "1.0.2",
                "2.15.3-alpha",
                "21.3.15-beta+12345",
            ]
        )


class JobInput(BaseModel):
    name: str
    location: HttpUrl


class CreateJob(BaseModel):
    name: str
    description: Optional[str]
    wdl_repo: HttpUrl
    version: str
    input_prefix: str
    inputs: List[JobInput]
    trigger: str

    @validator("version")
    def version_to_string(cls, v):
        if not PydanticVersion.isvalid(v):
            raise ValueError(f"{v} is not a semantic version")
        return str(v)


class UpdateJob(BaseModel):
    name: str
    wdl_repo: Optional[HttpUrl]
    version: str
    inputs: Optional[List[DirectoryPath]]
    updated_at: datetime = datetime.now()

    @validator("version")
    def version_to_string(cls, v):
        if not PydanticVersion.isvalid(v):
            raise ValueError(f"{v} is not a semantic version")
        return str(v)

    # @validator("version")
    # def version_later_than_current(
    #     cls, v: PydanticVersion, field: PydanticVersion
    # ):
    #     """Checks if new version of job is higher than old version"""
    #     if field >= v:
    #         raise valueerror(
    #             f"{str(v)} is not a higher version than {str(field)}"
    #         )
    #     return str(v)


class JobOut(UpdateJob):
    """Job fields returend to the client"""

    name: str
    version: str


class FetchJob(BaseModel):
    name: str


class ListJob(BaseModel):
    limit: int = 25
    offset: int = 0
    name: Optional[str]
    wdl_repo: Optional[HttpUrl]
    version: Optional[str]
    inputs: Optional[List[DirectoryPath]]
    updated_before: Optional[datetime]
    updated_after: Optional[datetime]
    created_before: Optional[datetime]
    created_after: Optional[datetime]


class JobQuery(ABC):
    @abstractmethod
    async def create_job(self, job: CreateJob) -> JobOut:
        pass

    @abstractmethod
    async def update_job(self, update: UpdateJob) -> JobOut:
        pass
