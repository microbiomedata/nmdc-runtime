# nmdc_runtime/site/change_sheets/gold_changesheet_generator.py
"""
gold_changesheet_generator.py: Provides classes to generate and validate changesheets for NMDC database objects
with missing or incorrect GOLD-derived metadata.
"""
import csv
import logging
import os
from typing import ClassVar, Dict, Any, Optional, Union
from nmdc_runtime.site.changesheets.changesheet_generator import (
    BaseChangesheetGenerator, JSON_OBJECT,
    ChangesheetLineItem, get_nmdc_biosample_by_id)


def get_gold_biosample_name_suffix(biosample: JSON_OBJECT) -> str:
    """
    Get the suffix for the name of a GOLD biosample - the last word in the biosampleName attribute
    e.g. "biosampleName": "Terrestrial soil microbial communities from
    Disney Wilderness Preserve, Southeast, FL, USA - DSNY_016-M-37-14-20140409-GEN-DNA1",

    Suffix = DSNY_016-M-37-14-20140409-GEN-DNA1

    :param biosample: a list of JSON objects representing GOLD biosamples
    :return: str
    """
    return biosample["biosampleName"].split()[-1]



def get_nmdc_biosample_object_id(nmdc_biosample: JSON_OBJECT) -> str:
    """
    Get the object_id of the given NMDC biosample
    :param nmdc_biosample: JSON_OBJECT
    :return: str
    """
    return nmdc_biosample["_id"]["$oid"]

# TODO: move this to a util module or common location
def get_normalized_gold_biosample_identifier(gold_biosample: JSON_OBJECT) -> str:
    """
    Get the normalized GOLD biosample identifier for the given GOLD biosample
    :param gold_biosample: JSON_OBJECT
    :return: str
    """
    return normalize_gold_biosample_id(gold_biosample["biosampleGoldId"])

def normalize_gold_biosample_id(gold_biosample_id: str) -> str:
    """
    Normalize the given GOLD biosample ID to the form "GOLD:<gold_biosample_id>"
    :param gold_biosample_id: str
    :return: str
    """
    if not gold_biosample_id.startswith("GOLD:"):
        gold_biosample_id = f"GOLD:{gold_biosample_id}"
    return gold_biosample_id

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
    #TODO: Connect to API and run query
    return []

def find_nmdc_omics_processing_via_gold_biosample_name_suffix(gold_biosample_name_suffix: str) -> Optional[JSON_OBJECT]:
    """
    Find the NMDC omics_processing that includes the given GOLD biosample name suffix in omics_processing_name
    :param gold_biosample_name_suffix: str
    :return: JSON_OBJECT
    """
    return None




class BaseGoldBiosampleChangesheetGenerator(BaseChangesheetGenerator):
    """
    Class for generating changesheets for GOLD-derived metadata
    """

    def __init__(self, name: str, gold_biosamples: list[JSON_OBJECT]) -> None:
        super().__init__(name)
        self.gold_biosamples = gold_biosamples

        self.gold_biosample_names = [get_gold_biosample_name_suffix(x) for x in self.gold_biosamples]


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


def _check_gold_ecosystem_metadata(nmdc_biosample: JSON_OBJECT, gold_biosample: JSON_OBJECT) -> list[ChangesheetLineItem]:
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
    for k,v in ecosystem_keys.items():
        if not nmdc_biosample.get(k):
            line_items.append(ChangesheetLineItem(
                get_nmdc_biosample_object_id(nmdc_biosample),
                "update",
                k,
                gold_biosample.get(v)))
    return line_items

def _check_gold_biosample_identifiers(nmdc_biosample: JSON_OBJECT, gold_biosample: JSON_OBJECT) -> list[ChangesheetLineItem]:
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
            get_nmdc_biosample_object_id(nmdc_biosample),
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
                 omics_to_biosample_map_file: Union[str, bytes, os.PathLike]) -> None:
        super().__init__(name, gold_biosamples)

        self.omics_processing_to_biosamples_map = {}
        with open(omics_to_biosample_map_file) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                self.omics_processing_to_biosamples_map[row["OmicsProcessing"]] = row["Biosamples"].split("|")



        logging.basicConfig(filename=self.output_filename_root + ".log", level=logging.INFO)

    def generate_changesheet(self) -> None:
        """
        Generate a changesheet for issue #397
        :return: None
        """
        for biosample in self.gold_biosamples:
            biosample_name_sfx = get_gold_biosample_name_suffix(biosample)
            gold_biosample_id = get_normalized_gold_biosample_identifier(biosample)

            # We don't expect to find any NMDC biosamples with the given gold_biosample_id
            # but just to be sure...
            nmdc_biosamples = find_nmdc_biosamples_by_gold_biosample_id(gold_biosample_id)
            if nmdc_biosamples:
                for nmdc_biosample in nmdc_biosamples:
                    line_items = compare_biosamples(nmdc_biosample, biosample)
                    for line_item in line_items:
                        self.add_changesheet_line_item(line_item)

            else:
            # Try to find an NMDC OmicsProcessing with the given biosample_name_sfx in the name
                logging.info(f"Could not find NMDC biosample with gold_biosample_id {biosample_name_sfx}")
                omics_processing = find_nmdc_omics_processing_via_gold_biosample_name_suffix(biosample_name_sfx)

                if not omics_processing:
                    logging.info(f"Could not find NMDC omics_processing with name {biosample_name_sfx}")
                    continue
                else:
                    logging.info(f"Found NMDC omics_processing with name {biosample_name_sfx}")

                    # TODO: Check if gold_sequencing_project_identifiers is missing

                    # TODO: Get gold_sequencing_project_identifiers from GOLD API by biosample["biosampleGoldId"]

                    #TODO: Add gold_sequencing_project_identifiers to NMDC omics_processing







    def validate_changesheet(self) -> bool:
        """
        Validate the changesheet for issue #397
        :return: bool
        """
        pass


