from pydantic import BaseModel, Extra


class ChangesheetIn(BaseModel):
    name: str
    content_type: str
    text: str


class Doc(BaseModel):
    class Config:
        extra = Extra.allow
