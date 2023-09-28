# nmdc_runtime/site/changesheets/__init__.py
from dagster import Definitions, EnvVar


from nmdc_runtime.site.changesheets.assets import (
    gold_biosamples_for_study,
    GoldApiResource,
)


defs = Definitions(
    assets=[gold_biosamples_for_study,],
    resources={
        "gold_api_resource": GoldApiResource(
            base_url=EnvVar("GOLD_API_BASE_URL"),
            username=EnvVar("GOLD_API_USERNAME"),
            password=EnvVar("GOLD_API_PASSWORD"),
        ),
    },
)
