from pydantic import BaseModel


class Globus(BaseModel):
    """
    Represents a Globus file transfer configuration.
    """

    task_id: str
    task_status: str


class SequencingProject(BaseModel):
    """
    Represents metadata for a sequencing project.
    """
    project_name: str
    proposal_id: str
    nmdc_study_id: str
    analysis_projects_dir: str