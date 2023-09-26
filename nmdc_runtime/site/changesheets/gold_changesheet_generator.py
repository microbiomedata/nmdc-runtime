# nmdc_runtime/site/change_sheets/gold_changesheet_generator.py
"""
gold_changesheet_generator.py: Provides classes to generate and validate changesheets for NMDC database objects
with missing or incorrect GOLD-derived metadata.
"""
import csv
from dagster import graph, op
import logging
import os
from pathlib import Path
from nmdc_runtime.site.resources import (
    runtime_api_site_client_resource,
    gold_api_client_resource,
)
from typing import Dict, Optional, List, Any
from nmdc_runtime.site.changesheets.changesheet_generator import (
    BaseChangesheetGenerator)
from nmdc_runtime.site.changesheets.changesheets import ChangesheetLineItem, get_nmdc_biosample_by_id, JSON_OBJECT
from nmdc_runtime.site.ops import gold_biosamples_by_study
from nmdc_runtime.site.site_utils.gold import get_gold_biosample_name_suffix, get_normalized_gold_biosample_identifier, \
    normalize_gold_biosample_id

resource_defs = {
    "runtime_api_site_client": runtime_api_site_client_resource,
    "gold_api_client": gold_api_client_resource,
}

preset_normal = {
    "config": {
        "resources": {
            "runtime_api_site_client": {
                "config": {
                    "base_url": {"env": "API_HOST"},
                    "site_id": {"env": "API_SITE_ID"},
                    "client_id": {"env": "API_SITE_CLIENT_ID"},
                    "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                },
            },
            "gold_api_client": {
                "config": {
                    "base_url": {"env": "GOLD_API_BASE_URL"},
                    "username": {"env": "GOLD_API_USERNAME"},
                    "password": {"env": "GOLD_API_PASSWORD"},
                },
            },
        },
        "ops": {},
    },
    "resource_defs": resource_defs,
}


def find_nmdc_biosamples_by_gold_biosample_id(gold_biosample_id: str) -> list[JSON_OBJECT]:
    """
    Find the NMDC biosample that includes the given GOLD biosample ID in gold_biosample_identifiers
    :param gold_biosample_id: str
    :return: JOSN_OBJECT
    """
    gold_biosample_id = normalize_gold_biosample_id(gold_biosample_id)
    query = {
        "find": "biosample_set",
        "filter": {"gold_biosample_identifiers": {"$elemMatch": {"$eq": gold_biosample_id}}}
    }
    # TODO: Connect to API and run query
    return []


def find_nmdc_omics_processing_via_gold_biosample_name_suffix(gold_biosample_name_suffix: str) -> Optional[JSON_OBJECT]:
    """
    Find the NMDC omics_processing that includes the given GOLD biosample name suffix in omics_processing.name
    :param gold_biosample_name_suffix: str
    :return: JSON_OBJECT
    """
    query = {
        "find": "omics_processing_set",
        "filter": {"name": {"$regex": f".*{gold_biosample_name_suffix}.*"}}
    }
    # TODO: Connect to API and run query
    return None


def get_nmdc_biosample_by_id(id: str) -> Optional[JSON_OBJECT]:
    """
    Get an NMDC biosample by ID eg nmdc:bsm-11-ecn7ad34
    :param id_: str
    :return: JSON_OBJECT
    """
    query = {
        "find": "biosample_set",
        "filter": {"id": id}
    }
    return None


def find_gold_sequencing_project_ids_by_gold_biosample_id(gold_biosample_id: str) -> [str]:
    """
    Find the GOLD sequencing project identifier(s) for the given GOLD biosample ID
    :param gold_biosample_id: str
    :return: JSON_OBJECT
    """
    gold_biosample_id = normalize_gold_biosample_id(gold_biosample_id)
    # Request URL
    # https://gold-ws.jgi.doe.gov/api/v1/projects?biosampleGoldId=Gb0255526
    return []


class BaseGoldBiosampleChangesheetGenerator(BaseChangesheetGenerator):
    """
    Class for generating changesheets for GOLD-derived metadata, starting with a list of GOLD biosamples
    """

    def __init__(self, name: str, gold_biosamples: list[JSON_OBJECT]) -> None:
        super().__init__(name)
        self.gold_biosamples = gold_biosamples


def compare_biosamples(nmdc_biosample: JSON_OBJECT, gold_biosample: JSON_OBJECT) -> list[ChangesheetLineItem]:
    """
    Compare the given NMDC and GOLD biosamples
    :param nmdc_biosample: JSON_OBJECT
    :param gold_biosample: JSON_OBJECT
    :return: list[ChangesheetLineItem]
    """

    line_items = []

    # Check the ecosystem metadata
    line_items.extend(_check_gold_ecosystem_metadata(nmdc_biosample, gold_biosample))

    # Check the GOLD biosample identifiers
    line_items.extend(_check_gold_biosample_identifiers(nmdc_biosample, gold_biosample))

    return line_items


def _check_gold_ecosystem_metadata(nmdc_biosample: JSON_OBJECT, gold_biosample: JSON_OBJECT) -> list[
    ChangesheetLineItem]:
    """
    Check the ecosystem metadata of the given NMDC and GOLD biosamples
    :param nmdc_biosample: JSON_OBJECT
    :param gold_biosample: JSON_OBJECT
    :return: list[ChangesheetLineItem]
    """
    # NMDC to GOLD ecosystem metadata mapping
    ecosystem_keys = {
        "ecosystem": "ecosystem",
        "ecosystem_category": "ecosystemCategory",
        "ecosystem_type": "ecosystemType",
        "ecosystem_subtype": "ecosystemSubtype",
    }
    line_items = []
    for k, v in ecosystem_keys.items():
        if not nmdc_biosample.get(k):
            line_items.append(ChangesheetLineItem(
                nmdc_biosample["id"],
                "update",
                k,
                gold_biosample.get(v)))
    return line_items


def _check_gold_biosample_identifiers(nmdc_biosample: JSON_OBJECT, gold_biosample: JSON_OBJECT) -> list[
    ChangesheetLineItem]:
    """
    Check the GOLD biosample identifiers of the given NMDC and GOLD biosamples
    :param nmdc_biosample: JSON_OBJECT
    :param gold_biosample: JSON_OBJECT
    :return: list[ChangesheetLineItem]
    """
    line_items = []
    gold_biosample_identifier = get_normalized_gold_biosample_identifier(gold_biosample)
    if not gold_biosample_identifier in nmdc_biosample.get("gold_biosample_identifiers"):
        line_items.append(ChangesheetLineItem(
            nmdc_biosample["id"],
            "insert",
            "gold_biosample_identifiers",
            gold_biosample_identifier))
    return line_items


class Issue397ChangesheetGenerator(BaseGoldBiosampleChangesheetGenerator):
    """
    Class for generating changesheet for issue #397

    omics_to_biosample_map_file is a file containing a mapping of omics_processing_id to biosample_id(s) separated by |
    e.g.
    OmicsProcessing	Biosamples
    nmdc:omprc-11-t0jqr240	nmdc:bsm-11-n4htkv94|nmdc:bsm-11-xkrpjq36
    """
    issue_link = "https://github.com/microbiomedata/issues/issues/397"

    def __init__(self, name: str, gold_biosamples: list[JSON_OBJECT],
                 omics_to_biosample_map: dict) -> None:
        super().__init__(name, gold_biosamples)
        self.omics_processing_to_biosamples_map = omics_to_biosample_map
        logging.basicConfig(filename=self.output_filename_root + ".log", level=logging.INFO)

    def generate_changesheet(self) -> None:
        """
        Generate a changesheet for issue #397
        :return: None
        """
        for gold_biosample in self.gold_biosamples:
            biosample_name_sfx = get_gold_biosample_name_suffix(gold_biosample)
            gold_biosample_id = get_normalized_gold_biosample_identifier(gold_biosample)

            # We don't expect to find any NMDC biosamples with the given gold_biosample_id
            # but just to be sure...
            nmdc_biosamples = find_nmdc_biosamples_by_gold_biosample_id(gold_biosample_id)
            if nmdc_biosamples:
                for nmdc_biosample in nmdc_biosamples:
                    line_items = compare_biosamples(nmdc_biosample, gold_biosample)
                    for line_item in line_items:
                        self.add_changesheet_line_item(line_item)

            else:
                # Try to find an NMDC OmicsProcessing with the given biosample_name_sfx in the name
                logging.info(f"Could not find NMDC biosample with gold_biosample_id {biosample_name_sfx}")
                omics_processing = find_nmdc_omics_processing_via_gold_biosample_name_suffix(biosample_name_sfx)
                if not omics_processing:
                    logging.info(f"Could not find NMDC omics_processing with name {biosample_name_sfx}")
                    continue

                logging.info(f"Found NMDC omics_processing with name {biosample_name_sfx}")

                # check for missing gold_sequencing_project_identifiers
                if not omics_processing.get("gold_sequencing_project_identifiers"):
                    gold_sequencing_project_identifiers = find_gold_sequencing_project_ids_by_gold_biosample_id(
                        gold_biosample_id)
                    if gold_sequencing_project_identifiers:
                        self.add_changesheet_line_item(ChangesheetLineItem(
                            omics_processing["_id"]["$oid"],
                            "insert items",
                            "gold_sequencing_project_identifiers",
                            gold_sequencing_project_identifiers))
                    else:
                        logging.info(f"Could not find GOLD sequencing project identifiers for {biosample_name_sfx}")

                # get corresponding NMDC biosample IDs from omics_processing_to_biosamples_map
                nmdc_biosample_ids = self.omics_processing_to_biosamples_map.get(
                    omics_processing["cursor"]["firstBatch"][0]["_id"]["$oid"])
                if not nmdc_biosample_ids:
                    logging.info(f"Could not find NMDC biosamples for omics_processing {omics_processing}")
                    continue

                for nmdc_biosample_id in nmdc_biosample_ids:
                    nmdc_biosample = get_nmdc_biosample_by_id(nmdc_biosample_id)
                    if not nmdc_biosample:
                        logging.info(f"Could not find NMDC biosample with id {nmdc_biosample_id}")
                        continue

                    line_items = compare_biosamples(nmdc_biosample, gold_biosample)
                    for line_item in line_items:
                        self.add_changesheet_line_item(line_item)

    def validate_changesheet(self) -> bool:
        """
        Validate the changesheet for issue #397
        :return: bool
        """
        # TODO: immpement this
        return True


@op
def read_omics_procesing_to_biosamples_data_file() -> Dict[str, list[str]]:
    """
    Get a map of omics_processing_id to biosample_id(s) separated by |

    :return: Dict[str, list[str]]
    """
    datafile = os.path.join(os.path.dirname(__file__), "data", "omics_processing_to_biosamples_map.tsv")
    omics_processing_to_biosamples_map = {}
    with open(datafile) as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        for row in reader:
            omics_processing_to_biosamples_map[row["OmicsProcessing"]] = row["Biosamples"].split("|")
    return omics_processing_to_biosamples_map


@op
def get_issue_397_gold_study_id(context) -> str:
    """
    Get the GOLD study ID for issue #397
    """
    ISSUE_397_GOLD_STUDY_ID = "Gs0114663"
    return ISSUE_397_GOLD_STUDY_ID


@op
def get_issue_397_changesheet_generator(context, gold_biosamples: List[Dict[str, Any]],
                                        omics_processing_to_biosamples_map) -> Issue397ChangesheetGenerator:
    """
    Initialize a changesheet generator for issue #397, generate the changesheet, and validate it
    :return: Issue397ChangesheetGenerator instance
    """
    changesheet_generator = Issue397ChangesheetGenerator("issue_397", gold_biosamples,
                                                         omics_processing_to_biosamples_map)
    changesheet_generator.generate_changesheet()
    if not changesheet_generator.validate_changesheet():
        raise Exception("Changesheet validation failed")

    return changesheet_generator


@graph
def generate_issue_397_changesheet():
    """
    Generate a changesheet for issue #397
    :return: None
    """
    study_id = get_issue_397_gold_study_id()
    omics_processing_to_biosamples_map = read_omics_procesing_to_biosamples_data_file()
    gold_biosamples = gold_biosamples_by_study(study_id)

    issue_397_changesheet_generator = get_issue_397_changesheet_generator(gold_biosamples,
                                                                          omics_processing_to_biosamples_map)


#
test_generate_issue_397_changesheet_job = generate_issue_397_changesheet.to_job(**preset_normal)
