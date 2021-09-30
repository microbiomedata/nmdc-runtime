from pydantic import BaseModel


class ChangesheetIn(BaseModel):
    name: str
    content_type: str
    text: str
