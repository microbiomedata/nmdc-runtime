import json
import pytest

from nmdc_runtime.site.changesheets.changesheet_generator import (
    BaseChangesheetGenerator, ChangesheetLineItem
)

from nmdc_runtime.util import REPO_ROOT_DIR
TEST_DATA_DIR = REPO_ROOT_DIR.joinpath("tests", "test_changesheets", "data")


@pytest.fixture
def gold_biosample_response():
    with open(TEST_DATA_DIR.joinpath("gold_biosample_response.json")) as f:
        return json.load(f)

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