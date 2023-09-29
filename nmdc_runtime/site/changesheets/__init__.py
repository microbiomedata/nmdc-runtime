# nmdc_runtime/site/changesheets/__init__.py
from dagster import Definitions, EnvVar


from nmdc_runtime.site.changesheets.assets import (
    gold_biosamples_for_study,
    omics_processing_to_biosamples_map,
)
from nmdc_runtime.site.changesheets.resources import GoldApiResource, RuntimeApiUserResource

defs = Definitions(
    assets=[
        gold_biosamples_for_study,
        omics_processing_to_biosamples_map,
    ],
    resources={
        "gold_api_resource": GoldApiResource(
            base_url=EnvVar("GOLD_API_BASE_URL"),
            username=EnvVar("GOLD_API_USERNAME"),
            password=EnvVar("GOLD_API_PASSWORD"),
        ),
        "runtime_api_user_resource": RuntimeApiUserResource(
            base_url=EnvVar("API_HOST"),
            username=EnvVar("API_ADMIN_USER"),
            password=EnvVar("API_ADMIN_PASS"),
        ),

    },
)
