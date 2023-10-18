# nmdc_runtime/site/normalization/gold.py
"""
gold.py: Provides functions to normalize and validate JGI GOLD data.
"""
import logging
from typing import Dict, Any

JSON_OBJECT = Dict[str, Any]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def get_gold_biosample_name_suffix(biosample_name: str) -> str:
    """
    Get the suffix for the name of a GOLD biosample - the last word in the biosampleName attribute
    e.g. "biosampleName": "Terrestrial soil microbial communities from
    Disney Wilderness Preserve, Southeast, FL, USA - DSNY_016-M-37-14-20140409-GEN-DNA1",

    Suffix = DSNY_016-M-37-14-20140409-GEN-DNA1

    :param biosample_name: str - the value of the biosampleName attribute
    :return: str
    """
    return biosample_name.split()[-1]


def normalize_gold_id(gold_id: str) -> str:
    """
    Normalize the given GOLD ID - e.g. biosample Gb012345 to the form
    "gold:Gb012345"
    :param gold_id: str - the GOLD ID to normalize
    :return: str - the normalized GOLD ID
    """
    if gold_id.startswith("gold:"):
        return gold_id
    elif gold_id.upper().startswith("GOLD:"):
        return f"gold:{gold_id[5:]}"
    elif gold_id.startswith("G") and ":" not in gold_id:
        return f"gold:{gold_id}"

    return gold_id
