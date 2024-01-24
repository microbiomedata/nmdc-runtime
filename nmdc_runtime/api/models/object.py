import datetime
import hashlib
import http
from enum import Enum
from typing import Optional, List, Dict

from pydantic import (
    field_validator,
    model_validator,
    Field,
    StringConstraints,
    BaseModel,
    AnyUrl,
    HttpUrl,
    field_serializer,
)
from typing_extensions import Annotated


class AccessMethodType(str, Enum):
    s3 = "s3"
    gs = "gs"
    ftp = "ftp"
    gsiftp = "gsiftp"
    globus = "globus"
    htsget = "htsget"
    https = "https"
    file = "file"


class AccessURL(BaseModel):
    headers: Optional[Dict[str, str]] = None
    url: AnyUrl

    @field_serializer("url")
    def serialize_url(self, url: AnyUrl, _info):
        return str(url)


class AccessMethod(BaseModel):
    access_id: Optional[Annotated[str, StringConstraints(min_length=1)]] = None
    access_url: Optional[AccessURL] = None
    region: Optional[str] = None
    type: AccessMethodType = AccessMethodType.https

    @model_validator(mode="before")
    def at_least_one_of_access_id_and_url(cls, values):
        access_id, access_url = values.get("access_id"), values.get("access_url")
        if access_id is None and access_url is None:
            raise ValueError(
                "At least one of access_url and access_id must be provided."
            )
        return values


ChecksumType = Annotated[
    str,
    StringConstraints(
        pattern=rf"(?P<checksumtype>({'|'.join(sorted(hashlib.algorithms_guaranteed))}))"
    ),
]


class Checksum(BaseModel):
    checksum: Annotated[str, StringConstraints(min_length=1)]
    type: ChecksumType


DrsId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9._~\-]+$")]
PortableFilename = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9._\-]+$")]


class ContentsObject(BaseModel):
    contents: Optional[List["ContentsObject"]] = None
    drs_uri: Optional[List[AnyUrl]] = None
    id: Optional[DrsId] = None
    name: PortableFilename

    @model_validator(mode="before")
    def no_contents_means_single_blob(cls, values):
        contents, id_ = values.get("contents"), values.get("id")
        if contents is None and id_ is None:
            raise ValueError("no contents means no further nesting, so id required")
        return values

    @field_serializer("drs_uri")
    def serialize_url(self, drs_uri: Optional[List[AnyUrl]], _info):
        if drs_uri is not None and len(drs_uri) > 0:
            return [str(u) for u in drs_uri]
        return drs_uri


ContentsObject.update_forward_refs()

Mimetype = Annotated[str, StringConstraints(pattern=r"^\w+/[-+.\w]+$")]
SizeInBytes = Annotated[int, Field(strict=True, ge=0)]


class Error(BaseModel):
    msg: Optional[str] = None
    status_code: http.HTTPStatus


class DrsObjectBase(BaseModel):
    aliases: Optional[List[str]] = None
    description: Optional[str] = None
    mime_type: Optional[Mimetype] = None
    name: Optional[PortableFilename] = None


class DrsObjectIn(DrsObjectBase):
    access_methods: Optional[List[AccessMethod]] = None
    checksums: List[Checksum]
    contents: Optional[List[ContentsObject]] = None
    created_time: datetime.datetime
    size: SizeInBytes
    updated_time: Optional[datetime.datetime] = None
    version: Optional[str] = None

    @model_validator(mode="before")
    def no_contents_means_single_blob(cls, values):
        contents, access_methods = values.get("contents"), values.get("access_methods")
        if contents is None and access_methods is None:
            raise ValueError(
                "no contents means single blob, which requires access_methods"
            )
        return values

    @field_validator("checksums")
    @classmethod
    def at_least_one_checksum(cls, v):
        if not len(v) >= 1:
            raise ValueError("At least one checksum requried")
        return v


class DrsObject(DrsObjectIn):
    id: DrsId
    self_uri: AnyUrl

    @field_serializer("self_uri")
    def serialize_url(self, self_uri: AnyUrl, _info):
        return str(self_uri)


Seconds = Annotated[int, Field(strict=True, gt=0)]


class ObjectPresignedUrl(BaseModel):
    url: HttpUrl
    expires_in: Seconds = 300

    @field_serializer("url")
    def serialize_url(self, url: HttpUrl, _info):
        return str(url)


class DrsObjectOutBase(DrsObjectBase):
    checksums: List[Checksum]
    created_time: datetime.datetime
    id: DrsId
    self_uri: AnyUrl
    size: SizeInBytes
    updated_time: Optional[datetime.datetime] = None
    version: Optional[str] = None

    @field_serializer("self_uri")
    def serialize_url(self, self_uri: AnyUrl, _info):
        return str(self_uri)


class DrsObjectBlobOut(DrsObjectOutBase):
    access_methods: List[AccessMethod]


class DrsObjectBundleOut(DrsObjectOutBase):
    access_methods: Optional[List[AccessMethod]] = None
    contents: List[ContentsObject]
