# nmdc_runtime/site/changesheets/assets.py
"""
assets.py: Provides software-defined assets for creating changesheets for NMDC database objects.
"""
import json
from pathlib import Path
from typing import List, Tuple

from dagster import asset, Config, AssetIn, materialize, AssetExecutionContext

from nmdc_runtime.site.changesheets.base import JSON_OBJECT, ChangesheetLineItem, Changesheet
from nmdc_runtime.site.normalization.gold import get_gold_biosample_name_suffix, normalize_gold_id
from nmdc_runtime.site.changesheets.resources import GoldApiResource, RuntimeApiUserResource

GOLD_NEON_SOIL_STUDY_ID = "Gs0144570"
DATA_PATH = Path(__file__).parent.joinpath("data")


@asset
def omics_processing_to_biosamples_map(context) -> dict:
    """
    Map omics processing to biosamples
    :return: dict
    """
    omics_processing_to_biosamples = {}
    with open(DATA_PATH.joinpath("OmicsProcessing-to-catted-Biosamples.tsv")) as f:
        lines_read = 0
        lines_skipped = 0
        for line in f:
            if not line.startswith("nmdc:"):
                lines_skipped += 1
                continue
            lines_read += 1
            omics_processing_id, biosample_ids = line.strip().split("\t")
            omics_processing_to_biosamples[omics_processing_id] = biosample_ids.split("|")
    context.log.info(f"Read {lines_read} lines from OmicsProcessing-to-catted-Biosamples.tsv")
    context.log.info(f"Skipped {lines_skipped} lines from OmicsProcessing-to-catted-Biosamples.tsv")
    return omics_processing_to_biosamples




class GoldBiosamplesForStudyConfig(Config):
    """
    Configuration for fetching GOLD biosamples for a study
    """
    study_id: str = GOLD_NEON_SOIL_STUDY_ID

@asset
def gold_biosamples_for_study(gold_api_resource: GoldApiResource,
                              config: GoldBiosamplesForStudyConfig) -> list[JSON_OBJECT]:
    """
    Biosamples for a GOLD study
    """
    # context.log.info(f"Fetching GOLD biosamples for study {config.study_id}")
    client = gold_api_resource.get_client()
    gold_biosamples = client.fetch_biosamples_by_study(config.study_id)
    # context.log.info(f"Fetched {len(gold_biosamples)} biosamples for study {config.study_id}")
    return gold_biosamples

# materialize([gold_biosamples_for_study,], run_config={"resources": {"gold_api_resource": {"config": {"study_id": "Gs0144570"}}}})
#

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
    client = runtime_api_user_resource.get_client()
    for gold_biosample in gold_biosamples:
        nmdc_biosamples = []
        gold_biosample_id = normalize_gold_biosample_id(gold_biosample["biosampleId"])
        for nmdc_biosample in client.get_biosamples_by_gold_biosample_id(gold_biosample_id):
            nmdc_biosamples.append(nmdc_biosample)
        gold_to_nmdc_biosamples.append((gold_biosample, nmdc_biosamples))
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
def gold_nmdc_missing_ecosystem_metadata(
        resolved_biosample_pairs: List[Tuple[JSON_OBJECT, JSON_OBJECT]],
) -> List[ChangesheetLineItem]:
    """
    NMDC biosamples for changesheet
    :param resolved_biosample_pairs:
    :return: ChangesheetLineItems
    """
    nmdc_biosamples = []
    return nmdc_biosamples


# {
# "find": "omics_processing_set",
# "filter": {"name": {"$regex": "ABBY_070-M-20170606-COMP-DNA1"}}
# }