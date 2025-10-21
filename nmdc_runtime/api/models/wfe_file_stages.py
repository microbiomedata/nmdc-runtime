from pydantic import BaseModel
class Globus(BaseModel):
    """
    Represents a Globus file transfer configuration.
    """

    task_id: str
    task_status: str
