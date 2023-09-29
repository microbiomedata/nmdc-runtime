# nmdc_runtime/site/changesheets/assets.py
"""
assets.py: Provides software-defined assets for creating changesheets for NMDC database objects.
"""
from dagster import asset, Config

from nmdc_runtime.site.changesheets.resources import GoldApiResource
from pathlib import Path
import json

GOLD_NEON_SOIL_STUDY_ID = "Gs0144570"
DATA_PATH = Path(__file__).parent.joinpath("data")


@asset
def omics_processing_to_biosamples_map() -> dict:
    """
    Map omics processing to biosamples
    :return: dict
    """
    with open(DATA_PATH.joinpath("omics_processing_to_biosamples_map.json")) as f:
        return json.load(f)


class GoldBiosamplesForStudyConfig(Config):
    """
    Configuration for fetching GOLD biosamples for a study
    """
    study_id: str

@asset
def gold_biosamples_for_study(gold_api_resource: GoldApiResource, config: GoldBiosamplesForStudyConfig) -> None:
    """
    Biosamples for a GOLD study
    """
    client = gold_api_resource.get_client()
    gold_biosamples = client.fetch_biosamples_by_study(config.study_id)


# {
# "find": "omics_processing_set",
# "filter": {"name": {"$regex": "ABBY_070-M-20170606-COMP-DNA1"}}
# }