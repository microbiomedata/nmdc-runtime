from datetime import datetime, timezone

from toolz import get_in

from nmdc_runtime.api.models.object_type import ObjectType
from nmdc_runtime.util import nmdc_jsonschema

_raw = [
    {
        "id": "read_qc_analysis_activity_set",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        # "description": "JSON documents satisfying schema for readqc analysis activity",
    },
    {
        "id": "metagenome_sequencing_activity_set",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        # "description": "JSON documents satisfying schema for metagenome sequencing activity",
    },
    {
        "id": "mags_activity_set",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        # "description": "JSON documents satisfying schema for mags activity",
    },
    {
        "id": "metagenome_annotation_activity_set",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        # "description": "JSON documents satisfying schema for metagenome annotation activity",
    },
    {
        "id": "metagenome_assembly_set",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        # "description": "JSON documents satisfying schema for metagenome assembly activity",
    },
    {
        "id": "read_based_taxonomy_analysis_activity_set",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "metaP analysis activity",
        # "description": "JSON documents satisfying schema for read based analysis activity",
    },
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
    {
        "id": "metatranscriptome_raw_paired_end_reads",
        "created_at": datetime(2021, 9, 7, tzinfo=timezone.utc),
        "name": "Metatranscriptome Raw Paired-End Reads Workflow Input",
        "description": "workflow input 2",
    },
    {
        "id": "gcms-metab-input",
        "created_at": datetime(2021, 9, 7, tzinfo=timezone.utc),
        "name": "Raw GCMS MetaB Input",
        "description": "",
    },
    {
        "id": "gcms-metab-calibration",
        "created_at": datetime(2021, 9, 7, tzinfo=timezone.utc),
        "name": "Raw GCMS MetaB Calibration",
        "description": "",
    },
    {
        "id": "nom-input",
        "created_at": datetime(2021, 9, 7, tzinfo=timezone.utc),
        "name": "Raw FTMS MetaB Input",
        "description": "",
    },
    {
        "id": "test",
        "created_at": datetime(2021, 9, 7, tzinfo=timezone.utc),
        "name": "A test object type",
        "description": "For use in unit and integration tests",
    },
    {
        "id": "metadata-changesheet",
        "created_at": datetime(2021, 9, 30, tzinfo=timezone.utc),
        "name": "metadata changesheet",
        "description": "Specification for changes to existing metadata",
    },
]

_raw.extend(
    [
        {
            "id": key,
            "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
            "name": key,
            # "description": spec["description"],
        }
        for key, spec in nmdc_jsonschema["properties"].items()
        if key.endswith("_set")
    ]
)
_raw.append(
    {
        "id": "schema#/definitions/Database",
        "created_at": datetime(2021, 9, 14, tzinfo=timezone.utc),
        "name": "Bundle of one or more metadata `*_set`s.",
        "description": get_in(
            ["definitions", "Database", "description"], nmdc_jsonschema
        ),
    }
)


def construct():
    return [ObjectType(**kwargs) for kwargs in _raw]
