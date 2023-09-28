# nmdc_runtime/site/changesheets/__init__.py
from dagster import Definitions

from nmdc_runtime.site.changesheets.assets import (
    gold_biosamples_by_study,
    GoldApiResource,
)


defs = Definitions(
    assets=[gold_biosamples_by_study],
    resources={
        "gold_api_resource": GoldApiResource,
    },
)
