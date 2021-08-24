from datetime import datetime, timezone

from nmdc_runtime.api.models.object_type import ObjectType

_raw = [
    {
        "id": "metadata-in",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "metadata submission",
        "description": "Input to the portal ETL process",
    },
    {
        "id": "metaproteomics_analysis_activity_set",
        "created_at": datetime(2021, 8, 23, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        "description": "JSON documents satisfying schema for metaproteomics analysis activity",
    },
    {
        "id": "metagenome_raw_paired_end_reads",
        "created_at": datetime(2021, 8, 24, tzinfo=timezone.utc),
        "name": "Metagenome Raw Paired-End Reads Workflow Input",
        "description": "workflow input",
    },
]


def construct():
    return [ObjectType(**kwargs) for kwargs in _raw]
