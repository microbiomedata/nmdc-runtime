from datetime import datetime, timezone

from nmdc_runtime.api.models.workflow import Workflow

_raw = [
    {
        "id": "readsqc-1.0.1",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Reads QC Workflow (v1.0.1)",
    },
    {
        "id": "rba-1.0.1",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Read-based Analysis (v1.0.1)",
    },
    {
        "id": "mgasmb-1.0.1",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metagenome Assembly (v1.0.1)",
    },
    {
        "id": "mganno-1.0.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "Metagenome Annotation (v1.0.0)",
    },
    {
        "id": "mgasmbgen-1.0.2",
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
        "id": "portal-etl-1.0.0",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "NMDC Portal ETL (v1.0.0)",
        "description": "Dagster-based pipeline(s) to transform metadata.",
    },
]


def construct():
    models = []
    for kwargs in _raw:
        kwargs["capability_ids"] = [kwargs["id"]]
        models.append(Workflow(**kwargs))
    return models
