import json
import os
import pytest

from nmdc_runtime.site.changesheets.changesheet_generator import (
    BaseChangesheetGenerator
)
from nmdc_runtime.site.changesheets.changesheets import ChangesheetLineItem

from nmdc_runtime.util import REPO_ROOT_DIR

TEST_DATA_DIR = REPO_ROOT_DIR.joinpath("tests", "test_changesheets", "data")
BONA_009_GOLD_BIOSAMPLES_API_RESPONSE = TEST_DATA_DIR.joinpath("bona_009_gold_biosamples_api_response.json")
BONA_009_BIOSAMPLES_NMDC_QUERY_API_RESPONSE = TEST_DATA_DIR.joinpath("bona_009_biosamples_nmdc_query_api_response.json")


@pytest.fixture
def gold_biosample_response():
    with open(TEST_DATA_DIR.joinpath("gold_biosample_response.json")) as f:
        return json.load(f)


@pytest.fixture
def gold_bona_009_biosample():
    with open(TEST_DATA_DIR.joinpath(BONA_009_GOLD_BIOSAMPLES_API_RESPONSE)) as f:
        response = json.load(f)
        return response[0]


@pytest.fixture
def omics_processing_to_biosamples_map():
    return os.path.join(TEST_DATA_DIR, "omics_processing_to_biosamples_map.tsv")

@pytest.fixture
def expected_omics_processing_to_biosamples_map():
    return {
        "nmdc:omprc-11-t0jqr240": ["nmdc:bsm-11-n4htkv94", "nmdc:bsm-11-xkrpjq36"],
        "nmdc:omprc-11-c4v85b38": ["nmdc:bsm-11-e552n350", "nmdc:bsm-11-mg8ehr76", "nmdc:bsm-11-v15pnd34"]
    }


@pytest.fixture
def nmdc_bona_009_biosamples():
    with open(TEST_DATA_DIR.joinpath(BONA_009_BIOSAMPLES_NMDC_QUERY_API_RESPONSE)) as f:
        response = json.load(f)
        return response["cursor"]["firstBatch"]


@pytest.fixture
def nmdc_bona_009_biosample():
    with open(TEST_DATA_DIR.joinpath(BONA_009_BIOSAMPLES_NMDC_QUERY_API_RESPONSE)) as f:
        response = json.load(f)
        biosample = response["cursor"]["firstBatch"][0]
        return biosample


@pytest.fixture
def nmdc_bona_009_biosample_no_ecosystem_metadata():
    ecosystem_keys = [
        "ecosystem", "ecosystem_category", "ecosystem_type", "ecosystem_subtype",
    ]
    with open(TEST_DATA_DIR.joinpath(BONA_009_BIOSAMPLES_NMDC_QUERY_API_RESPONSE)) as f:
        response = json.load(f)
        biosample = response["cursor"]["firstBatch"][0]
        for key in ecosystem_keys:
            biosample.pop(key)
        return biosample

@pytest.fixture
def nmdc_bona_009_biosample_no_gold_biosample_identifiers():
    with open(TEST_DATA_DIR.joinpath(BONA_009_BIOSAMPLES_NMDC_QUERY_API_RESPONSE)) as f:
        response = json.load(f)
        biosample = response["cursor"]["firstBatch"][0]
        biosample["gold_biosample_identifiers"] = []
        return biosample


@pytest.fixture
def gold_biosample_expected_names():
    return [
        "DSNY_016-M-37-14-20140409-GEN-DNA1",
        "GRSM_007-O-20160720-COMP-DNA1",
        "GUAN_006-M-20161005-COMP-DNA1",
        "HARV_020-O-20170731-COMP-DNA1",
    ]


@pytest.fixture
def base_changesheet_generator():
    return BaseChangesheetGenerator("test_changesheet_generator")


@pytest.fixture
def insert_line_item():
    return ChangesheetLineItem("test_id:01234", "insert", "some_attribute", "some_value")

@pytest.fixture
def bona_009_no_ecosystem_metadata_expected_changesheet_line_items():
    return [
        ChangesheetLineItem(
            "nmdc:bsm-11-a1q56a63",
            "update",
            "ecosystem",
            "Environmental",
        ),
        ChangesheetLineItem(
            "nmdc:bsm-11-a1q56a63",
            "update",
            "ecosystem_category",
            "Terrestrial",
        ),
        ChangesheetLineItem(
            "nmdc:bsm-11-a1q56a63",
            "update",
            "ecosystem_type",
            "Soil",
        ),
        ChangesheetLineItem(
            "nmdc:bsm-11-a1q56a63",
            "update",
            "ecosystem_subtype",
            "Boreal forest/Taiga",
        ),
    ]

@pytest.fixture
def bona_009_no_gold_biosample_identifiers_expected_changesheet_line_items():
    return [
        ChangesheetLineItem(
            "nmdc:bsm-11-a1q56a63",
            "insert",
            "gold_biosample_identifiers",
            "GOLD:Gb0356158",
        ),

    ]
