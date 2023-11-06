import datetime
from typing import Generic, TypeVar, Optional, List, Any, Union

from pydantic import StringConstraints, BaseModel, HttpUrl, field_serializer

from nmdc_runtime.api.models.util import ResultT
from typing_extensions import Annotated

MetadataT = TypeVar("MetadataT")


PythonImportPath = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.]+$")]


class OperationError(BaseModel):
    code: str
    message: str
    details: Any = None


class Operation(BaseModel, Generic[ResultT, MetadataT]):
    id: str
    done: bool = False
    expire_time: datetime.datetime
    result: Optional[Union[ResultT, OperationError]] = None
    metadata: Optional[MetadataT] = None


class UpdateOperationRequest(BaseModel, Generic[ResultT, MetadataT]):
    done: bool = False
    result: Optional[Union[ResultT, OperationError]] = None
    metadata: Optional[MetadataT] = {}


class ListOperationsResponse(BaseModel, Generic[ResultT, MetadataT]):
    resources: List[Operation[ResultT, MetadataT]]
    next_page_token: Optional[str] = None


class Result(BaseModel):
    model: Optional[PythonImportPath] = None


class EmptyResult(Result):
    pass


class Metadata(BaseModel):
    # XXX alternative: set model field using __class__ on __init__()?
    model: Optional[PythonImportPath] = None


class PausedOrNot(Metadata):
    paused: bool


class ObjectPutMetadata(Metadata):
    object_id: str
    site_id: str
    url: HttpUrl
    expires_in_seconds: int

    @field_serializer("url")
    def serialize_url(self, url: HttpUrl, _info):
        return str(url)
