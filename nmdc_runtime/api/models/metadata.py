from pydantic import ConfigDict, BaseModel


class ChangesheetIn(BaseModel):
    name: str
    content_type: str
    text: str


class Doc(BaseModel):
    model_config = ConfigDict(extra="allow")
