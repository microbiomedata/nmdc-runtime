import datetime
import hashlib
import http
from enum import Enum
from typing import Optional, List, Dict

from pydantic import (
    BaseModel,
    AnyUrl,
    constr,
    conint,
    HttpUrl,
    root_validator,
    validator,
)


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
    headers: Optional[Dict[str, str]]
    url: AnyUrl


class AccessMethod(BaseModel):
    access_id: Optional[constr(min_length=1)]
    access_url: Optional[AccessURL]
    region: Optional[str]
    type: AccessMethodType = AccessMethodType.https

    @root_validator
    def at_least_one_of_access_id_and_url(cls, values):
        access_id, access_url = values.get("access_id"), values.get("access_url")
        if access_id is None and access_url is None:
            raise ValueError(
                "At least one of access_url and access_id must be provided."
            )
        return values


ChecksumType = constr(
    regex=rf"(?P<checksumtype>({'|'.join(sorted(hashlib.algorithms_guaranteed))}))"
)


class Checksum(BaseModel):
    checksum: constr(min_length=1)
    type: ChecksumType


DrsId = constr(regex=r"^[A-Za-z0-9._~\-]+$")
PortableFilename = constr(regex=r"^[A-Za-z0-9._\-]+$")


class ContentsObject(BaseModel):
    contents: Optional[List["ContentsObject"]]
    drs_uri: Optional[List[AnyUrl]]
    id: Optional[DrsId]
    name: PortableFilename

    @root_validator()
    def no_contents_means_single_blob(cls, values):
        contents, id_ = values.get("contents"), values.get("id")
        if contents is None and id_ is None:
            raise ValueError("no contents means no further nesting, so id required")
        return values


ContentsObject.update_forward_refs()

Mimetype = constr(regex=r"^\w+/[-+.\w]+$")
SizeInBytes = conint(strict=True, ge=0)


class Error(BaseModel):
    msg: Optional[str]
    status_code: http.HTTPStatus


class DrsObjectBase(BaseModel):
    aliases: Optional[List[str]]
    description: Optional[str]
    mime_type: Optional[Mimetype]
    name: Optional[PortableFilename]


class DrsObjectIn(DrsObjectBase):
    access_methods: Optional[List[AccessMethod]]
    checksums: List[Checksum]
    contents: Optional[List[ContentsObject]]
    created_time: datetime.datetime
    size: SizeInBytes
    updated_time: Optional[datetime.datetime]
    version: Optional[str]

    @root_validator()
    def no_contents_means_single_blob(cls, values):
        contents, access_methods = values.get("contents"), values.get("access_methods")
        if contents is None and access_methods is None:
            raise ValueError(
                "no contents means single blob, which requires access_methods"
            )
        return values

    @validator("checksums")
    def at_least_one_checksum(cls, v):
        if not len(v) >= 1:
            raise ValueError("At least one checksum requried")
        return v


class DrsObject(DrsObjectIn):
    id: DrsId
    self_uri: AnyUrl


Seconds = conint(strict=True, gt=0)


class ObjectPresignedUrl(BaseModel):
    url: HttpUrl
    expires_in: Seconds = 300


class DrsObjectOutBase(DrsObjectBase):
    checksums: List[Checksum]
    created_time: datetime.datetime
    id: DrsId
    self_uri: AnyUrl
    size: SizeInBytes
    updated_time: Optional[datetime.datetime]
    version: Optional[str]


class DrsObjectBlobOut(DrsObjectOutBase):
    access_methods: List[AccessMethod]


class DrsObjectBundleOut(DrsObjectOutBase):
    access_methods: Optional[List[AccessMethod]]
    contents: List[ContentsObject]
