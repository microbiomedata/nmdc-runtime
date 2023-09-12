# nmdc_runtime/site/change_sheets/gold_changesheet_generator.py
"""
gold_changesheet_generator.py: Provides classes to generate and validate changesheets for NMDC database objects
with missing or incorrect GOLD-derived metadata.
"""
import logging
from typing import ClassVar, Dict, Any, Optional
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


def find_nmdc_biosample_by_gold_biosample_id(gold_biosample_id: str) -> Optional[JSON_OBJECT]:
    """
    Find the NMDC biosample that includes the given GOLD biosample ID in gold_biosample_identifiers
    :param gold_biosample_id: str
    :return: JOSN_OBJECT
    """
    return None

def find_nmdc_biosample_id_via_omics_processing_name(gold_biosample_name_suffix: str) -> Optional[str]:
    """
    Find the NMDC biosample ID via the omics_processing name, searching for the given GOLD biosample name suffix
    :param gold_biosample_name_suffix:
    :return: nmdc_
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


class Issue397ChangesheetGenerator(BaseGoldBiosampleChangesheetGenerator):
    """
    Class for generating changesheet for issue #397
    """
    issue_link = "https://github.com/microbiomedata/issues/issues/397"

    def __init__(self, name: str, gold_biosamples: list[JSON_OBJECT]) -> None:
        super().__init__(name, gold_biosamples)
        logging.basicConfig(filename=self.output_filename_root + ".log", level=logging.INFO)

    def compare_biosamples(self, nmdc_biosample: JSON_OBJECT, gold_biosample: JSON_OBJECT) -> list[ChangesheetLineItem]:
        """
        Compare the given NMDC and GOLD biosamples
        :param nmdc_biosample: JSON_OBJECT
        :param gold_biosample: JSON_OBJECT
        :return: bool
        """
        return []

    def generate_changesheet(self) -> None:
        """
        Generate a changesheet for issue #397
        :return: None
        """
        for biosample in self.gold_biosamples:
            biosample_name_sfx = get_gold_biosample_name_suffix(biosample)

            nmdc_biosample = find_nmdc_biosample_by_gold_biosample_id(biosample_name_sfx)
            if nmdc_biosample:
                line_items = self.compare_biosamples(nmdc_biosample, biosample)
                for line_item in line_items:
                    self.add_changesheet_line_item(line_item)
            else:
                logging.info(f"Could not find NMDC biosample with gold_biosample_id {biosample_name_sfx}")
                nmdc_biosample_id = find_nmdc_biosample_id_via_omics_processing_name(biosample_name_sfx)
                if nmdc_biosample_id:
                    logging.info(f"Found NMDC biosample with omics_processing name {biosample_name_sfx}")
                    nmdc_biosample = get_nmdc_biosample_by_id(nmdc_biosample_id)
                    if nmdc_biosample:
                        line_items = self.compare_biosamples(nmdc_biosample, biosample)
                        for line_item in line_items:
                            self.add_changesheet_line_item(line_item)
                    else:
                        logging.info(f"Could not find NMDC biosample with id {nmdc_biosample_id}")


    def validate_changesheet(self) -> bool:
        """
        Validate the changesheet for issue #397
        :return: bool
        """
        pass


