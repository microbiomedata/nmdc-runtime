from typing import List, Optional

from pydantic import BaseModel


class OpenLineageBase(BaseModel):
    producer: str
    schemaURL: str


class RunRequest(BaseModel):
    job_id: str
    inputs: List[str] = []


class JobSummary(OpenLineageBase):
    id: str
    description: str


class Run(BaseModel):
    id: str


class RunSummary(OpenLineageBase):
    id: str
    status: str
    started_at_time: str
    was_started_by: str
    inputs: List[str]
    outputs: List[str]
    job: JobSummary


class RunEvent(OpenLineageBase):
    run: Run
    job: JobSummary
    type: str
    time: str
    inputs: List[str]
    outputs: Optional[List[str]] = []
