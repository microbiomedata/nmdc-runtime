from pydantic import BaseModel, Extra
from fastapi import UploadFile, File


class ChangesheetIn(BaseModel):
    name: str
    content_type: str
    text: str


class Doc(BaseModel):
    class Config:
        extra = Extra.allow


class GffIn(BaseModel):
    md5sum: str
    activity_id: str
