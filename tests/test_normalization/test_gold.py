# nmdc-runtime/tests/test_normalization/test_gold.py
"""
test_gold.py: Tests for nmdc_runtime/site/normalization/gold.py
"""

from nmdc_runtime.site.normalization.gold import (
    get_gold_biosample_name_suffix,
    normalize_gold_id,
)


def test_get_gold_biosample_name_suffix():
    biosample_name = "Terrestrial soil microbial communities from Disney Wilderness Preserve, Southeast, FL, USA - DSNY_016-M-37-14-20140409-GEN-DNA1"
    assert (
        get_gold_biosample_name_suffix(biosample_name)
        == "DSNY_016-M-37-14-20140409-GEN-DNA1"
    )


def test_normalize_gold_biosample_id():
    test_inputs = [
        ("GOLD:Gb0356158", "gold:Gb0356158"),
        ("Gb0356158", "gold:Gb0356158"),
        ("gold:Gb0356158", "gold:Gb0356158"),
        ("Gold:Gb0356158", "gold:Gb0356158"),
    ]
    for test_input, expected_output in test_inputs:
        assert normalize_gold_id(test_input) == expected_output
