#!/usr/bin/env python
# nmdc_runtime/site/changesheets/scripts/missing_neon_soils_ecosystem_data.py
"""
missing_neon_soils_ecosystem_data.py: Create a changesheet for missing ecosystem data for NEON soils samples
"""

import logging
import os
from pathlib import Path

import click
from dotenv import load_dotenv

from nmdc_runtime.site.normalization.gold import (
    get_gold_biosample_name_suffix,
    normalize_gold_biosample_id,
)
from nmdc_runtime.site.resources import GoldApiClient, RuntimeApiUserClient
from nmdc_runtime.site.changesheets.base import Changesheet, ChangesheetLineItem

load_dotenv()
GOLD_NEON_SOIL_STUDY_ID = "Gs0144570"
DATA_PATH = Path(__file__).parent.parent.joinpath("data")


def read_omics_processing_to_biosample_map() -> dict:
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
            omics_processing_to_biosamples[omics_processing_id] = biosample_ids.split(
                "|"
            )
    logging.info(
        f"Read {lines_read} lines from OmicsProcessing-to-catted-Biosamples.tsv"
    )
    logging.info(
        f"Skipped {lines_skipped} lines from OmicsProcessing-to-catted-Biosamples.tsv"
    )
    return omics_processing_to_biosamples


@click.command()
@click.option("--study_id", default=GOLD_NEON_SOIL_STUDY_ID, help="GOLD study ID")
def generate_changesheet(study_id):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("starting missing_neon_soils_ecosystem_data.py...")
    logging.info(f"study_id: {study_id}")

    gold_api_client = GoldApiClient(
        base_url=os.getenv("GOLD_API_BASE_URL"),
        username=os.getenv("GOLD_API_USERNAME"),
        password=os.getenv("GOLD_API_PASSWORD"),
    )
    logging.info("connected to GOLD API...")

    runtime_api_user_client = RuntimeApiUserClient(
        base_url=os.getenv("API_HOST"),
        username=os.getenv("API_ADMIN_USER"),
        password=os.getenv("API_ADMIN_PASS"),
    )
    logging.info("connected to NMDC API...")

    omics_processing_to_biosamples = read_omics_processing_to_biosample_map()
    logging.info(
        f"read {len(omics_processing_to_biosamples)} omics processing to biosamples mappings..."
    )
    gold_biosamples = gold_api_client.fetch_biosamples_by_study(study_id)
    logging.info(f"retrieved {len(gold_biosamples)} biosamples from GOLD API...")

    for goldbs in gold_biosamples:
        logging.info(f"normalizing goldbs: {goldbs['biosampleGoldId']}...")
        goldbs_id = normalize_gold_biosample_id(goldbs["biosampleGoldId"])
        logging.info(f"normalized goldbs_id: {goldbs_id}")
        goldbs_name_suffix = get_gold_biosample_name_suffix(goldbs["biosampleName"])
        # logging.info(f"goldbs_id: {goldbs_id}")
        # logging.info(f"goldbs_name_suffix: {goldbs_name_suffix}")


if __name__ == "__main__":
    generate_changesheet()
