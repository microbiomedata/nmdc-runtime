#!/usr/bin/env python
# nmdc_runtime/site/changesheets/scripts/missing_neon_soils_ecosystem_data.py
"""
missing_neon_soils_ecosystem_data.py: Create a changesheet for missing ecosystem data for NEON soils samples
"""

import logging
import os
import time
from pathlib import Path
from typing import List, Tuple

import click
from dotenv import load_dotenv

from nmdc_runtime.site.changesheets.base import (
    Changesheet,
    ChangesheetLineItem,
    JSON_OBJECT,
)
from nmdc_runtime.site.normalization.gold import (
    get_gold_biosample_name_suffix,
    normalize_gold_id,
)
from nmdc_runtime.site.resources import GoldApiClient, RuntimeApiUserClient

load_dotenv()
GOLD_NEON_SOIL_STUDY_ID = "Gs0144570"
NAME = "missing_neon_soils_ecosystem_data"
# omics processing to biosamples file in data directory
DATA_PATH = Path(__file__).parent.parent.joinpath("data")

log_filename = f"{NAME}-{time.strftime('%Y%m%d-%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filename=log_filename,
    encoding="utf-8",
    filemode="w",
)


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
    logging.debug(
        f"Read {lines_read} lines from OmicsProcessing-to-catted-Biosamples.tsv"
    )
    logging.info(
        f"Skipped {lines_skipped} lines from OmicsProcessing-to-catted-Biosamples.tsv"
    )
    return omics_processing_to_biosamples


def gold_biosample_to_nmdc_biosamples_and_omics_processing_records(
    runtime_api_client, omprc_to_bs_map, goldbs
) -> Tuple[List[JSON_OBJECT], List[JSON_OBJECT]]:
    """
    Find the corresponding NMDC biosamples and omics processing records for a
    GOLD biosample
    :param runtime_api_client:
    :param omprc_to_bs_map: Dict of omics processing ID to biosample IDs
    :param goldbs: a GOLD biosample
    :return: (
            List of corresponding NMDC biosamples,
            List of corresponding NMDC omics processing records
            )
    """
    goldbs_id = normalize_gold_id(goldbs["biosampleGoldId"])
    goldbs_name_suffix = get_gold_biosample_name_suffix(goldbs["biosampleName"])
    logging.info(f"goldbs_id: {goldbs_id}")
    logging.info(f"goldbs_name_suffix: {goldbs_name_suffix}")

    # Search for NMDC biosamples with by GOLD biosample ID
    nmdc_biosamples = []
    logging.info(f"Searching for NMDC biosamples with {goldbs_id}...")
    nmdcbs = runtime_api_client.get_biosamples_by_gold_biosample_id(goldbs_id)
    logging.info(f"Found {len(nmdcbs)} NMDC biosamples with {goldbs_id}...")
    nmdc_biosamples.extend(nmdcbs)

    # Search for NMDC biosamples via omics processing name containing GOLD biosample name suffix
    logging.info(
        f"Searching for NMDC omics processing name containing {goldbs_name_suffix}..."
    )
    omprc_records = runtime_api_client.get_omics_processing_by_name(goldbs_name_suffix)
    for omprc in omprc_records:
        omprc_id = omprc["id"]
        logging.info(f"omprc_id: {omprc_id}")
        logging.info(
            f"Searching for NMDC biosamples with omics processing {omprc_id}..."
        )
        nmdcbs_ids = omprc_to_bs_map.get(omprc_id, [])
        logging.info(f"Found {len(nmdcbs_ids)} NMDC biosamples with {omprc_id}...")
        for nmdcbs_id in nmdcbs_ids:
            nmdcbs_req = runtime_api_client.request("GET", f"/biosamples/{nmdcbs_id}")
            if nmdcbs_req.status_code != 200:
                logging.error(
                    f"Failed to retrieve NMDC biosample {nmdcbs_id}: {nmdcbs_req.status_code}"
                )
                continue
            nmdcbs = nmdcbs_req.json()
            nmdc_biosamples.append(nmdcbs)

    logging.info(f"Found {len(nmdc_biosamples)} NMDC biosamples for {goldbs_id}...")
    return nmdc_biosamples, omprc_records


def compare_biosamples(goldbs, nmdcbs) -> List[ChangesheetLineItem]:
    changesheet_line_items = []

    # Check for missing ecosystem metadata
    changesheet_line_items.extend(_check_ecosystem_metadata(goldbs, nmdcbs))

    # Check for missing gold biosample identifiers
    changesheet_line_items.extend(_check_gold_biosample_identifiers(goldbs, nmdcbs))

    return changesheet_line_items


def _check_ecosystem_metadata(goldbs, nmdcbs) -> List[ChangesheetLineItem]:
    # nmdc to gold ecosystem key map
    ecosystem_key_map = {
        "ecosystem": "ecosystem",
        "ecosystem_category": "ecosystemCategory",
        "ecosystem_type": "ecosystemType",
        "ecosystem_subtype": "ecosystemSubtype",
    }
    changesheet_line_items = []
    for nmdc_key, gold_key in ecosystem_key_map.items():
        if not gold_key in goldbs:
            logging.warning(f"no {gold_key} for {goldbs['biosampleGoldId']}...")
            continue
        if not nmdc_key in nmdcbs:
            changesheet_line_items.append(
                ChangesheetLineItem(
                    id=nmdcbs["id"],
                    action="update",
                    attribute=nmdc_key,
                    value=goldbs.get(gold_key),
                )
            )
            continue
        if nmdcbs[nmdc_key] != goldbs.get(gold_key):
            changesheet_line_items.append(
                ChangesheetLineItem(
                    id=nmdcbs["id"],
                    action="update",
                    attribute=nmdc_key,
                    value=goldbs.get(gold_key),
                )
            )
            continue

    return changesheet_line_items


def _check_gold_biosample_identifiers(goldbs, nmdcbs) -> List[ChangesheetLineItem]:
    changesheet_line_items = []
    goldbs_id = normalize_gold_id(goldbs["biosampleGoldId"])
    if not goldbs_id in nmdcbs["gold_biosample_identifiers"]:
        changesheet_line_items.append(
            ChangesheetLineItem(
                id=nmdcbs["id"],
                action="insert",
                attribute="gold_biosample_identifiers",
                value=goldbs_id + "|",
            )
        )
    return changesheet_line_items


def compare_projects(gold_project, omprc_record) -> ChangesheetLineItem:
    gold_project_id = normalize_gold_id(gold_project["projectGoldId"])
    if "gold_sequencing_project_identifiers" not in omprc_record:
        return ChangesheetLineItem(
            id=omprc_record["id"],
            action="insert",
            attribute="gold_sequencing_project_identifiers",
            value=gold_project_id + "|",
        )

    if gold_project_id not in omprc_record["gold_sequencing_project_identifiers"]:
        return ChangesheetLineItem(
            id=omprc_record["id"],
            action="insert",
            attribute="gold_sequencing_project_identifiers",
            value=gold_project_id + "|",
        )


@click.command()
@click.option("--study_id", default=GOLD_NEON_SOIL_STUDY_ID, help="GOLD study ID")
@click.option("--use_dev_api", is_flag=True, default=False, help="Use the dev API")
def generate_changesheet(study_id, use_dev_api):
    """
    Generate a changesheet for missing ecosystem data for NEON soils samples by:
    1. Retrieving GOLD biosamples for the given study
    2. Finding the corresponding NMDC biosamples and omics processing records for each GOLD biosample
    3. Comparing the GOLD biosample to the NMDC biosamples and omics processing records
    4. Generating a changesheet for the differences

    WARNING: This script is not idempotent. It will generate a new changesheet each time it is run.

    Changesheet is written to nmdc_runtime/site/changesheets/changesheets_output

    :param study_id: The GOLD study ID
    :param use_dev_api: Use the dev API (default: False)
    :return:
    """
    start_time = time.time()
    logging.info("starting missing_neon_soils_ecosystem_data.py...")
    logging.info(f"study_id: {study_id}")

    gold_api_client = GoldApiClient(
        base_url=os.getenv("GOLD_API_BASE_URL"),
        username=os.getenv("GOLD_API_USERNAME"),
        password=os.getenv("GOLD_API_PASSWORD"),
    )
    logging.info("connected to GOLD API...")

    if use_dev_api:
        base_url = os.getenv("API_HOST_DEV")
        logging.info("using dev API...")
    else:
        base_url = os.getenv("API_HOST")
        logging.info("using prod API...")

    runtime_api_user_client = RuntimeApiUserClient(
        base_url=base_url,
        username=os.getenv("API_QUERY_USER"),
        password=os.getenv("API_QUERY_PASS"),
    )
    logging.info("connected to NMDC API...")

    # Retrieve GOLD biosamples for the given study
    gold_biosamples = gold_api_client.fetch_biosamples_by_study(study_id)
    logging.info(f"retrieved {len(gold_biosamples)} biosamples from GOLD API...")

    # omics processing to biosamples map generated by a SPARQL query
    omprc_to_bs_map = read_omics_processing_to_biosample_map()

    changesheet = Changesheet(name=NAME)
    # For each GOLD biosample, find the corresponding NMDC biosamples
    nmdcbs_count = 0
    unfindable_goldbs_ids = []
    for goldbs in gold_biosamples:
        (
            nmdc_biosamples,
            omprc_records,
        ) = gold_biosample_to_nmdc_biosamples_and_omics_processing_records(
            runtime_api_user_client, omprc_to_bs_map, goldbs
        )
        if not nmdc_biosamples:
            logging.warning(
                f"no corresponding NMDC biosamples found for {goldbs['biosampleGoldId']}..."
            )
            unfindable_goldbs_ids.append(goldbs["biosampleGoldId"])
            continue
        logging.info(
            f"found {len(nmdc_biosamples)} corresponding NMDC biosamples for {goldbs['biosampleGoldId']}..."
        )
        nmdcbs_count += len(nmdc_biosamples)
        for nmdcbs in nmdc_biosamples:
            logging.info(f"nmdcbs: {nmdcbs['id']}")
            changesheet.line_items.extend(compare_biosamples(goldbs, nmdcbs))

        # Insert gold project id into omprc alternative identifiers
        gold_projects = gold_api_client.request(
            "/projects", params={"biosampleGoldId": goldbs["biosampleGoldId"]}
        )
        for gold_project in gold_projects:
            for omprc_record in omprc_records:
                changesheet.line_items.append(
                    compare_projects(gold_project, omprc_record)
                )

    logging.info(f"Processed {len(gold_biosamples)} GOLD biosamples...")
    logging.info(f"found {nmdcbs_count} corresponding NMDC biosamples...")
    logging.info(f"unfindable_count: {len(unfindable_goldbs_ids)}...")
    for unfindable_goldbs_ids in unfindable_goldbs_ids:
        logging.info(f"unfindable_goldbs_id: {unfindable_goldbs_ids}...")
    logging.info(f"changesheet has {len(changesheet.line_items)} line items...")

    changesheet.write_changesheet()

    logging.info("Validating changesheet...")
    is_valid_changesheet = changesheet.validate_changesheet(base_url)
    logging.info(f"Changesheet is valid: {is_valid_changesheet}")

    logging.info(
        f"missing_neon_soils_ecosystem_data.py completed in {time.time() - start_time} seconds..."
    )


if __name__ == "__main__":
    generate_changesheet()
