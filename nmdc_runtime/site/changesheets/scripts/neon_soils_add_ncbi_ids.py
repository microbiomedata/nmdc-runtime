#!/usr/bin/env python3
# coding: utf-8
# nmdc_runtime/site/changesheets/scripts/neon_soils_add_ncbi_ids.py
"""
neon_soils_add_ncbi_ids.py: Add NCBI biosample IDs to neon soils biosamples, and
add NCBI study ID to neon soils study.
"""
import logging
import os
from pathlib import Path
import time

import click
from dotenv import load_dotenv

from nmdc_runtime.site.changesheets.base import (
    Changesheet,
    ChangesheetLineItem,
    JSON_OBJECT,
)

from nmdc_runtime.site.resources import GoldApiClient, RuntimeApiUserClient

load_dotenv()
NAME = "neon_soils_add_ncbi_ids"
NMDC_STUDY_ID = "nmdc:sty-11-34xj1150"

log_filename = f"{NAME}-{time.strftime('%Y%m%d-%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    filename=log_filename, encoding="utf-8", filemode="w", )


@click.command()
@click.option("--study_id", default=NMDC_STUDY_ID, help="NMDC study ID")
@click.option(
    "--use_dev_api", is_flag=True, default=False, help="Use the dev API"
)
def generate_changesheet(study_id, use_dev_api):
    """
    Generate a changesheet for neon soils study and biosamples by:
    1. Retrieving all biosamples for neon soils study
    2. For each biosample, retrieve the corresponding GOLD biosample record
    3. Retrieve the NCBI biosample ID from the GOLD biosample record
    4. Generate a changesheet for the neon soils biosamples, adding the NCBI IDs
    5. Add changesheet line item for NCDB study ID

    WARNING: This script is not idempotent. It will generate a new changesheet
    each time it is run.
    Changesheet is written to nmdc_runtime/site/changesheets/changesheets_output

    :param study_id: The NMDC study ID
    :param use_dev_api: Use the dev API (default: False)
    :return:
    """
    start_time = time.time()
    logging.info(f"Generating changesheet for {study_id}")
    logging.info(f"Using dev API: {use_dev_api}")

    # Initialize the NMDC API
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

    # Initialize the GOLD API
    gold_api_client = GoldApiClient(
        base_url=os.getenv("GOLD_API_BASE_URL"),
        username=os.getenv("GOLD_API_USERNAME"),
        password=os.getenv("GOLD_API_PASSWORD"),
    )
    logging.info("connected to GOLD API...")

    # Retrieve all biosamples for the neon soils study
    biosamples = runtime_api_user_client.get_biosamples_for_study(study_id)
    logging.info(f"retrieved {len(biosamples)} biosamples for {study_id}")



if __name__ == "__main__":
    generate_changesheet()
