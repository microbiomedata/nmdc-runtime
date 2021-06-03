from datetime import datetime, timezone

from nmdc_runtime.api.models.trigger import Trigger

_raw = [
    {
        "created_at": datetime(2021, 6, 1, tzinfo=timezone.utc),
        "object_type_id": "metadata-in",
        "workflow_id": "portal-etl-1.0.0",
    },
]


def construct():
    models = []
    for kwargs in _raw:
        kwargs["id"] = f'{kwargs["object_type_id"]}--{kwargs["workflow_id"]}'
        models.append(Trigger(**kwargs))
    return models
