# nmdc_runtime/site/changesheets/__init__.py
from dagster import Definitions, EnvVar


from nmdc_runtime.site.changesheets.assets import (
    gold_biosamples_for_study,
    omics_processing_to_biosamples_map,
    gold_to_nmdc_biosamples_by_gold_identifier,
    gold_to_nmdc_biosamples_by_omics_processing_name,
    resolved_gold_to_nmdc_biosample_pairs,
    gold_nmdc_missing_ecosystem_metadata
)
from nmdc_runtime.site.changesheets.resources import GoldApiResource, RuntimeApiUserResource



defs = Definitions(
    assets=[
        gold_biosamples_for_study,
        omics_processing_to_biosamples_map,
        gold_to_nmdc_biosamples_by_gold_identifier,
        gold_to_nmdc_biosamples_by_omics_processing_name,
        resolved_gold_to_nmdc_biosample_pairs,
        gold_nmdc_missing_ecosystem_metadata,

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
