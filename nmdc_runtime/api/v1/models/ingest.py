from typing import List, Optional

from pydantic import BaseModel

from components.workflow.workflow.core import (
    DataObject,
    MetaGenomeSequencingActivity,
)


class Ingest(BaseModel):
    data_object_set: List[DataObject] = []
    metagenome_sequencing_activity_set: List[MetaGenomeSequencingActivity] = []
