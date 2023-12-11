#!/usr/bin/env python3
# coding: utf-8
# nmdc_runtime/site/changesheets/scripts/neon_soils_add_ncbi_ids.py
"""
neon_soils_add_ncbi_ids.py: Add NCBI biosample accessions to neon soils
biosamples, NCBI bioproject accessions to omics processing, and
NCBI Umbrella bioproject accession to neon soils study.
"""
import logging
import time

import click
from dotenv import load_dotenv

from nmdc_runtime.site.changesheets.base import (
    Changesheet,
    ChangesheetLineItem,
    get_gold_client,
    get_runtime_client,
)

load_dotenv()
NAME = "neon_soils_add_ncbi_ids"
NMDC_STUDY_ID = "nmdc:sty-11-34xj1150"
UMBRELLA_BIOPROJECT_ACCESSION = "PRJNA1029061"

log_filename = f"{NAME}-{time.strftime('%Y%m%d-%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filename=log_filename,
    encoding="utf-8",
    filemode="w",
)


def _get_change_for_biosample(biosample, ncbi_biosample_accession):
    """
    Get the changes for the given biosample
    :param biosample: dict - the biosample
    :param ncbi_biosample_accession: str - the NCBI BioSample accession
    :return: list - the changes
    """
    ncbi_biosample_accessions = biosample.get("insdc_biosample_identifiers", [])
    if ncbi_biosample_accession in ncbi_biosample_accessions:
        return
    biosample_id = biosample["id"]
    logging.info(f"creating change for biosample_id: {biosample_id}")
    return ChangesheetLineItem(
        id=biosample["id"],
        action="insert",
        attribute="insdc_biosample_identifiers",
        value="biosample:" + ncbi_biosample_accession + "|",
    )


def _get_change_for_omics_processing(
    omics_processing_record, ncbi_bioproject_accession
):
    """
    Get the changes for the given omics_processing_record
    :param omics_processing_record:
    :param ncbi_bioproject_accession:
    :return:
    """
    ncbi_bioproject_accessions = omics_processing_record.get(
        "insdc_bioproject_identifiers", []
    )
    if ncbi_bioproject_accession in ncbi_bioproject_accessions:
        return
    omics_processing_id = omics_processing_record["id"]
    logging.info(f"creating change for omics_processing_id: {omics_processing_id}")
    return ChangesheetLineItem(
        id=omics_processing_id,
        action="insert",
        attribute="insdc_bioproject_identifiers",
        value="bioproject:" + ncbi_bioproject_accession + "|",
    )


@click.command()
@click.option("--study_id", default=NMDC_STUDY_ID, help="NMDC study ID")
@click.option("--use_dev_api", is_flag=True, default=True, help="Use the dev API")
def generate_changesheet(study_id, use_dev_api):
    """
    Generate a changesheet for neon soils study and biosamples by:
    0. Changesheet line item: Umbrella BioProjectAccession to
        study.insdc_project_identifiers
    1. Retrieving all gold_study_identifiers for the neon soils study
    2. For each gold_study_identifier, retrieve the GOLD projects
    3. For each GOLD project,
        A. retrieve the corresponding NMDC biosample(s). For each biosample,
            - Changesheet line item:NCBI BioSampleAccession to
            insdc_biosample_identifiers
        B. Retrieve the corresponding NMDC omics_processing. For each,
            - Changesheet line item:NCBI BioProjectAccession to
            insdc_bioproject_identifiers

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
    runtime_client = get_runtime_client(use_dev_api)

    # Initialize the GOLD API
    gold_client = get_gold_client()

    # Initialize the changesheet
    changesheet = Changesheet(name=NAME)

    # 1. Retrieve all gold_study_identifiers for the neon soils study
    logging.info(f"Retrieving gold_study_identifiers for {study_id}")
    res = runtime_client.request("GET", f"/studies/{study_id}")
    nmdc_study = res.json()
    changesheet.line_items.append(
        ChangesheetLineItem(
            id=study_id,
            action="insert",
            attribute="insdc_bioproject_identifiers",
            value="bioproject:" + UMBRELLA_BIOPROJECT_ACCESSION + "|",
        )
    )

    gold_study_identifiers = nmdc_study["gold_study_identifiers"]
    logging.info(f"gold_study_identifiers: {gold_study_identifiers}")
    gold_project_count = 0
    biosample_count = 0
    for gold_study_identifier in gold_study_identifiers:
        # 2. For each gold_study_identifier, retrieve the GOLD projects
        if gold_study_identifier == "gold:Gs0144570":
            # TODO verify that this one has already been done
            continue
        logging.info(
            f"Retrieving GOLD projects for gold_study_identifier: {gold_study_identifier}"
        )
        projects = gold_client.fetch_projects_by_study(gold_study_identifier)
        logging.info(f"Retrieved {len(projects)} projects")

        # 3. For each GOLD project,
        for project in projects:
            gold_project_count += 1
            project_gold_id = project["projectGoldId"]
            biosample_gold_id = project["biosampleGoldId"]
            ncbi_bioproject_accession = project["ncbiBioProjectAccession"]
            ncbi_biosample_accession = project["ncbiBioSampleAccession"]

            # A. retrieve the corresponding NMDC biosample(s)
            logging.info(
                f"Retrieving NMDC biosamples for biosample_gold_id: {biosample_gold_id}"
            )
            biosamples = runtime_client.get_biosamples_by_gold_biosample_id(
                biosample_gold_id
            )
            logging.info(f"Retrieved {len(biosamples)} biosamples")
            for biosample in biosamples:
                biosample_count += 1
                biosample_id = biosample["id"]
                logging.info(f"biosample_id: {biosample_id}")
                # NcbiBioSampleAccession to insdc_biosample_identifiers
                change = _get_change_for_biosample(biosample, ncbi_biosample_accession)
                if change:
                    changesheet.line_items.append(change)

            # B. Retrieve the corresponding NMDC omics_processing
            logging.info(
                f"Retrieving NMDC omics_processing for project_gold_id: {project_gold_id}"
            )
            omics_processing_records = (
                runtime_client.get_omics_processing_records_by_gold_project_id(
                    project_gold_id
                )
            )
            logging.info(f"Retrieved {len(omics_processing_records)} omics_processings")
            for omics_processing in omics_processing_records:
                omics_processing_id = omics_processing["id"]
                logging.info(f"omics_processing_id: {omics_processing_id}")
                # NcbiBioProjectAccession to insdc_experiment_identifiers
                change = _get_change_for_omics_processing(
                    omics_processing, ncbi_bioproject_accession
                )
                if change:
                    changesheet.line_items.append(change)

    logging.info(f"gold_project_count: {gold_project_count}")
    logging.info(f"biosample_count: {biosample_count}")
    logging.info(f"changesheet has {len(changesheet.line_items)} line items")

    # Write the changesheet
    changesheet.write_changesheet()

    # Validate the changesheet
    if changesheet.validate_changesheet(runtime_client.base_url):
        logging.info(f"Changesheet is valid")
    else:
        logging.error(f"Changesheet is invalid")

    logging.info(f"Completed in {time.time() - start_time} seconds")


if __name__ == "__main__":
    generate_changesheet()
