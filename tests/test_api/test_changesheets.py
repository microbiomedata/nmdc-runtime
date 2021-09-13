import json
from functools import lru_cache
from typing import Optional

import pandas as pd
from toolz import get_in

from nmdc_runtime.api.core.changesheets import load_changesheet, update_data
from nmdc_runtime.util import REPO_ROOT_DIR

TEST_DATA_DIR = REPO_ROOT_DIR.joinpath("metadata-translation", "notebooks", "data")


@lru_cache
def load_studies() -> dict:
    studies = {}
    for i in (1, 2, 3):
        with open(TEST_DATA_DIR.joinpath(f"study-data{i}.json")) as f:
            s = json.load(f)
            studies[s["id"]] = s
    return studies


def get_study_by_id(id_: str) -> Optional[dict]:
    return load_studies().get(id_.strip())


def test_load_changesheet():
    df = load_changesheet(TEST_DATA_DIR.joinpath("changesheet-with-separator1.tsv"))
    assert isinstance(df, pd.DataFrame)


def test_toplevel_attribute_update():
    df = load_changesheet(TEST_DATA_DIR.joinpath("changesheet-with-separator1.tsv"))
    grouped = df.groupby("group_id")
    id_, df_change = list(grouped)[0]
    data = get_study_by_id(id_)
    assert data["name"] != "soil study"
    assert data["ecosystem"] != "soil"
    new_data = update_data(data, df_change, print_data=True)
    assert new_data["name"] == "soil study"
    assert new_data["ecosystem"] == "soil"


def test_nested_attribute_update():
    df = load_changesheet(TEST_DATA_DIR.joinpath("changesheet-with-separator1.tsv"))
    grouped = df.groupby("group_id")
    id_, df_change = list(grouped)[1]
    data = get_study_by_id(id_)
    assert data["name"] != "data for study 2"
    assert get_in(["doi", "has_raw_value"], data) != "10.9999/8888"
    new_data = update_data(data, df_change, print_data=True)
    assert new_data["name"] == "data for study 2"
    assert get_in(["doi", "has_raw_value"], new_data) == "10.9999/8888"
