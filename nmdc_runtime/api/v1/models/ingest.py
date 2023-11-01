from typing import List, Optional

from components.workflow.workflow.core import DataObject, ReadsQCSequencingActivity
from pydantic import BaseModel


class Ingest(BaseModel):
    data_object_set: List[DataObject] = []
    read_qc_analysis_activity_set: Optional[List[ReadsQCSequencingActivity]] = None
    metagenome_assembly_activity_set: Optional[List[ReadsQCSequencingActivity]] = None
    metagenome_annotation_activity_set: Optional[List[ReadsQCSequencingActivity]] = None
