from datetime import datetime, timezone

from nmdc_runtime.api.models.trigger import Trigger

_raw = [
    {
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "object_type_id": "metadata-in",
        "workflow_id": "metadata-in-1.0.0",
    },
    {
        "created_at": datetime(2021, 9, 1, tzinfo=timezone.utc),
        "object_type_id": "metaproteomics_analysis_activity_set",
        "workflow_id": "metap-metadata-1.0.0",
    },
    {
        "created_at": datetime(2021, 9, 1, tzinfo=timezone.utc),
        "object_type_id": "metagenome_raw_paired_end_reads",
        "workflow_id": "metag-1.0.0",
    },
    {
        "created_at": datetime(2021, 9, 7, tzinfo=timezone.utc),
        "object_type_id": "metatranscriptome_raw_paired_end_reads",
        "workflow_id": "metat-1.0.0",
    },
    {
        "created_at": datetime(2021, 9, 9, tzinfo=timezone.utc),
        "object_type_id": "test",
        "workflow_id": "test",
    },
    {
        "created_at": datetime(2021, 9, 20, tzinfo=timezone.utc),
        "object_type_id": "nom-input",
        "workflow_id": "nom-1.0.0",
    },
    {
        "created_at": datetime(2021, 9, 20, tzinfo=timezone.utc),
        "object_type_id": "gcms-metab-input",
        "workflow_id": "gcms-metab-1.0.0",
    },
    {
        "created_at": datetime(2021, 9, 30, tzinfo=timezone.utc),
        "object_type_id": "metadata-changesheet",
        "workflow_id": "apply-changesheet-1.0.0",
    },
    {
        "created_at": datetime(2022, 1, 20, tzinfo=timezone.utc),
        "object_type_id": "metagenome_sequencing_activity_set",
        "workflow_id": "mgrc-1.0.6",
    },
    {
        "created_at": datetime(2022, 1, 20, tzinfo=timezone.utc),
        "object_type_id": "metagenome_sequencing_activity_set",
        "workflow_id": "metag-1.0.0",
    },
    {
        "created_at": datetime(2022, 1, 20, tzinfo=timezone.utc),
        "object_type_id": "metagenome_annotation_activity_set",
        "workflow_id": "mags-1.0.4",
    },
    {
        "created_at": datetime(2022, 1, 20, tzinfo=timezone.utc),
        "object_type_id": "metagenome_assembly_set",
        "workflow_id": "mgann-1.0.0",
    },
    {
        "created_at": datetime(2022, 1, 20, tzinfo=timezone.utc),
        "object_type_id": "read_qc_analysis_activity_set",
        "workflow_id": "mgasm-1.0.3",
    },
    {
        "created_at": datetime(2022, 1, 20, tzinfo=timezone.utc),
        "object_type_id": "read_qc_analysis_activity_set",
        "workflow_id": "mgrba-1.0.2",
    },
]


def construct():
    models = []
    for kwargs in _raw:
        kwargs["id"] = f'{kwargs["object_type_id"]}--{kwargs["workflow_id"]}'
        models.append(Trigger(**kwargs))
    return models
