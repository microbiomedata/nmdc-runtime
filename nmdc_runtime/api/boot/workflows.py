from datetime import datetime, timezone

from nmdc_runtime.api.models.workflow import Workflow

_raw = [
    {
        "id": "metag-1.0.0",
        "created_at": datetime(2021, 8, 24, tzinfo=timezone.utc),
        "name": "Metagenome Analysis Workflow (v1.0.0)",
    },
    {
        "id": "readqc-1.0.6",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Reads QC Workflow (v1.0.1)",
    },
    {
        "id": "mags-1.0.4",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Read-based Analysis (v1.0.1)",
    },
    {
        "id": "mgrba-1.0.2",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Read-based Analysis (v1.0.1)",
    },
    {
        "id": "mgasm-1.0.3",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metagenome Assembly (v1.0.1)",
    },
    {
        "id": "mgann-1.0.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metagenome Annotation (v1.0.0)",
    },
    {
        "id": "mgasmbgen-1.0.1",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metagenome Assembled Genomes (v1.0.2)",
    },
    {
        "id": "metat-0.0.2",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metatranscriptome (v0.0.2)",
    },
    {
        "id": "metap-1.0.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metaproteomic (v1.0.0)",
    },
    {
        "id": "metab-2.1.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metabolomics  (v2.1.0)",
    },
    {
        "id": "gold-translation-1.0.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "GOLD db dump translation",
        "description": "Transform metadata obtained from the JGI GOLD database.",
    },
    {
        "id": "metap-metadata-1.0.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "metaP metadata ETL",
        "description": "Ingest and validate metaP metadata",
    },
    {
        "id": "metadata-in-1.0.0",
        "created_at": datetime(2021, 10, 12, tzinfo=timezone.utc),
        "name": "general metadata ETL",
        "description": "Validate and ingest metadata from JSON files",
    },
    {
        "id": "test",
        "created_at": datetime(2021, 9, 9, tzinfo=timezone.utc),
        "name": "A test workflow",
        "description": "For use in unit and integration tests",
    },
    {
        "id": "gcms-metab-1.0.0",
        "created_at": datetime(2021, 9, 20, tzinfo=timezone.utc),
        "name": "GCMS-based metabolomics",
    },
    {
        "id": "nom-1.0.0",
        "created_at": datetime(2021, 9, 20, tzinfo=timezone.utc),
        "name": "Natural Organic Matter characterization",
    },
    {
        "id": "apply-changesheet-1.0.0",
        "created_at": datetime(2021, 9, 30, tzinfo=timezone.utc),
        "name": "apply metadata changesheet",
        "description": "Validate and apply metadata changes from TSV/CSV files",
    },
    {
        "id": "export-study-biosamples-as-csv-1.0.0",
        "created_at": datetime(2022, 6, 8, tzinfo=timezone.utc),
        "name": "export study biosamples metadata as CSV",
        "description": "Export study biosamples metadata as CSV",
    },
    {
        "id": "gold_study_to_database",
        "created_at": datetime(2023, 2, 17, tzinfo=timezone.utc),
        "name": "Get nmdc:Database for GOLD study",
        "description": "For a given GOLD study ID, produce an nmdc:Database representing that study and related entities",
    },
]


def construct():
    models = []
    for kwargs in _raw:
        kwargs["capability_ids"] = []
        models.append(Workflow(**kwargs))
    return models
