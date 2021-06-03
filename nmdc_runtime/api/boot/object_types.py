from datetime import datetime, timezone

from nmdc_runtime.api.models.object_type import ObjectType

_raw = [
    {
        "id": "metadata-in",
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "name": "metadata submission",
        "description": "Input to the portal ETL process",
    },
]


def construct():
    return [ObjectType(**kwargs) for kwargs in _raw]
