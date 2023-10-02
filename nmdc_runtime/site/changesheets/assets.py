# nmdc_runtime/site/changesheets/assets.py
"""
assets.py: Provides software-defined assets for creating changesheets for NMDC database objects.
"""
import json
from pathlib import Path
from typing import List, Tuple

from dagster import asset, Config, AssetIn

from nmdc_runtime.site.changesheets.base import JSON_OBJECT, ChangesheetLineItem, Changesheet
from nmdc_runtime.site.changesheets.resources import GoldApiResource, RuntimeApiUserResource

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
def gold_biosamples_for_study(gold_api_resource: GoldApiResource,
                              config: GoldBiosamplesForStudyConfig) -> list[JSON_OBJECT]:
    """
    Biosamples for a GOLD study
    """
    client = gold_api_resource.get_client()
    gold_biosamples = client.fetch_biosamples_by_study(config.study_id)
    return gold_biosamples


@asset(ins={"gold_biosamples": AssetIn("gold_biosamples_for_study")})
def gold_to_nmdc_biosamples_by_gold_identifier(
        gold_biosamples: List[JSON_OBJECT],
        runtime_api_user_resource: RuntimeApiUserResource
) -> List[Tuple[JSON_OBJECT, List[JSON_OBJECT]]]:
    """
    Map GOLD biosample identifiers to NMDC biosample identifiers
    :param gold_biosamples: list
    :return: list
    """
    gold_to_nmdc_biosamples = []
    return gold_to_nmdc_biosamples

@asset(ins={
    "omics_processing_to_biosamples_map": AssetIn("omics_processing_to_biosamples_map", ),
    "gold_to_nmdc_biosamples": AssetIn("gold_to_nmdc_biosamples_by_gold_identifier"),})
def gold_to_nmdc_biosamples_by_omics_processing_name(
        omics_processing_to_biosamples_map: dict,
        gold_to_nmdc_biosamples: List[Tuple[JSON_OBJECT, List[JSON_OBJECT]]],
        runtime_api_user_resource: RuntimeApiUserResource
    ) -> List[Tuple[JSON_OBJECT, List[JSON_OBJECT]]]:
    """
    Map GOLD biosample identifiers to NMDC biosample identifiers
    :param gold_to_nmdc_biosamples: list
    :return: list
    """
    gold_to_nmdc_biosamples = []
    return gold_to_nmdc_biosamples

@asset(ins={
    "gold_to_nmdc_biosamples": AssetIn("gold_to_nmdc_biosamples_by_omics_processing_name")
    })
def resolved_gold_to_nmdc_biosample_pairs(
        gold_to_nmdc_biosamples: List[Tuple[JSON_OBJECT, List[JSON_OBJECT]]],
        runtime_api_user_resource: RuntimeApiUserResource
    ) -> List[Tuple[JSON_OBJECT, JSON_OBJECT]]:
    """
    Resolve GOLD to NMDC biosample pairs
    :param gold_to_nmdc_biosamples: list
    :return: list
    """
    gold_nmdc_biosample_pairs = []
    return gold_nmdc_biosample_pairs

@asset(ins={
    "resolved_biosample_pairs": AssetIn("resolved_gold_to_nmdc_biosample_pairs"),
})
def gold_nmdc_missing_ecosystem_values_for_changesheet(
        resolved_biosample_pairs: List[Tuple[JSON_OBJECT, JSON_OBJECT]],
) -> List[ChangesheetLineItem]:
    """
    NMDC biosamples for changesheet
    :param resolved_biosample_pairs: list
    :return: list
    """
    nmdc_biosamples = []
    return nmdc_biosamples


# {
# "find": "omics_processing_set",
# "filter": {"name": {"$regex": "ABBY_070-M-20170606-COMP-DNA1"}}
# }