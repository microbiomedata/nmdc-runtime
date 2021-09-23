import json
from functools import lru_cache
from typing import Optional

import pandas as pd
from toolz import dissoc

from nmdc_runtime.api.core.metadata import load_changesheet, update_mongo_db
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
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
    mdb = get_mongo(run_config_frozen__normal_env).db
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv"), mdb
    )
    assert isinstance(df, pd.DataFrame)


def test_update_01():
    mdb = get_mongo(run_config_frozen__normal_env).db
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")
    assert study_doc["name"] != "soil study"
    assert study_doc["ecosystem"] != "soil"
    test_db_name = "nmdc_runtime_test"
    mdb.client.drop_database(test_db_name)
    mdb_scratch = mdb.client[test_db_name]
    mdb_scratch.study_set.insert_one(study_doc)
    status = update_mongo_db(df, mdb_scratch)
    assert status["nModified"] == 9
    new_study_doc = dissoc(mdb_scratch.study_set.find_one({"id": id_}), "_id")
    assert new_study_doc["name"] == "soil study"
    assert new_study_doc["ecosystem"] == "soil"
