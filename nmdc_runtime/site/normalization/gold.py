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

    :param biosample: a list of JSON objects representing GOLD biosamples
    :return: str
    """
    return biosample_name.split()[-1]


def normalize_gold_biosample_id(gold_biosample_id: str) -> str:
    """
    Normalize the given GOLD biosample ID to the form "GOLD:<gold_biosample_id>"
    :param gold_biosample_id: str
    :return: str
    """
    if gold_biosample_id.startswith("GOLD:"):
        return gold_biosample_id
    elif gold_biosample_id.startswith("Gb"):
        return f"GOLD:{gold_biosample_id}"
    elif gold_biosample_id.startswith("gold:") or gold_biosample_id.startswith("Gold:"):
        return f"GOLD:{gold_biosample_id[5:]}"
    return gold_biosample_id
