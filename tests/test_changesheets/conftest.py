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
def gold_biosample_expected_names(gold_biosample_response):
    return [biosample["biosampleName"].split()[-1] for biosample in gold_biosample_response]

@pytest.fixture
def base_changesheet_generator():
    return BaseChangesheetGenerator("test_changesheet_generator")

@pytest.fixture
def insert_line_item():
    return ChangesheetLineItem("test_id:01234", "insert", "some_attribute", "some_value")