# nmdc_runtime/site/site_utils/gold.py
"""
gold.py: Provides functions to normalize and validate JGI GOLD data.
"""
from nmdc_runtime.site.changesheets.changesheets import JSON_OBJECT


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
