from typing import List, Optional

from pydantic import BaseModel

from components.workflow.workflow.core import (
    DataObject,
    ReadsQCSequencingActivity,
)


class Ingest(BaseModel):
    data_object_set: List[DataObject] = []
    reads_qc_analysis_activity_set: List[ReadsQCSequencingActivity] = []
