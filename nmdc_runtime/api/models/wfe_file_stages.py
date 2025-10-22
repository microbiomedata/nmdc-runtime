from pydantic import BaseModel, Field


class GlobusTask(BaseModel):
    """
    Represents a Globus file transfer configuration.
    """

    task_id: str = Field( 
        ..., description="ID from Globus of the task", examples=["Some task id"] 
    ) 
    task_status: str = Field( 
        ..., description="Status of the globus task.", examples=["Some status"] 
    ) 
