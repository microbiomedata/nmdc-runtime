import datetime
import http
from enum import Enum
from typing import Optional, List, Dict, Union

from pydantic import BaseModel, Json, AnyUrl, constr, conint, HttpUrl


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
    headers: Optional[List[Json[Dict[str, str]]]]
    url: AnyUrl


class AccessMethod(BaseModel):
    access_id: Optional[str]
    access_url: Optional[AccessURL]
    region: Optional[str]
    type: AccessMethodType


ChecksumType = (
    str  # Cannot be an Enum because "sha-256" (contains a dash) is a valid value.
)


class Checksum(BaseModel):
    checksum: str
    type: ChecksumType


DrsId = constr(regex=r"^[A-Za-z0-9.-_~]+$")
PortableFilename = constr(regex=r"^[A-Za-z0-9.-_]+$")


class ContentsObject(BaseModel):
    contents: Optional[List["ContentsObject"]]
    drs_uri: Optional[List[AnyUrl]]
    id: Optional[DrsId]
    name: PortableFilename


ContentsObject.update_forward_refs()

Mimetype = constr(regex=r"^\w+/[-+.\w]+$")
SizeInBytes = conint(strict=True, ge=0)


class DrsObject(BaseModel):
    access_methods: Optional[List[AccessMethod]]
    aliases: Optional[List[str]]
    checksums: List[Checksum]
    contents: Optional[List[ContentsObject]]
    created_time: datetime.datetime
    description: Optional[str]
    id: DrsId
    mime_type: Optional[Mimetype]
    name: Optional[PortableFilename]
    self_uri: AnyUrl
    size: SizeInBytes
    updated_time: Optional[datetime.datetime]
    version: Optional[str]


class Error(BaseModel):
    msg: Optional[str]
    status_code: http.HTTPStatus


class DrsObjectBase(BaseModel):
    aliases: Optional[List[str]]
    description: Optional[str]
    mime_type: Optional[Mimetype] = "application/json"
    name: Optional[PortableFilename]


class DrsObjectBlobIn(DrsObjectBase):
    site_id: str


class DrsObjectBundleIn(DrsObjectBase):
    contents: List[ContentsObject]


DrsObjectIn = Union[DrsObjectBlobIn, DrsObjectBundleIn]


Seconds = conint(strict=True, gt=0)


class DrsObjectPresignedUrlPut(BaseModel):
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
