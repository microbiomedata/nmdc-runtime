import json
from functools import lru_cache
from typing import Optional

import pandas as pd
from toolz import dissoc

from nmdc_runtime.api.core.metadata import (
    load_changesheet,
    update_mongo_db,
    mongo_update_command_for,
    copy_docs_in_update_cmd,
)
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
    update_cmd = mongo_update_command_for(df)
    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_result = results[0]
    assert first_result["update_info"]["nModified"] == 9
    assert first_result["doc_after"]["name"] == "soil study"
    assert first_result["doc_after"]["ecosystem"] == "soil"
    assert first_result["validation_errors"] == []


def test_changesheet_array_item_nested_attributes():
    mdb = get_mongo(run_config_frozen__normal_env).db
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-array-item-nested-attributes.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")
    assert study_doc.get("has_credit_associations", []) == []
    update_cmd = mongo_update_command_for(df)
    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_doc_after = results[0]["doc_after"]
    assert "has_credit_associations" in first_doc_after
    assert len(first_doc_after["has_credit_associations"]) == 1
    assert first_doc_after["has_credit_associations"] == [
        {
            "applied_role": "Conceptualization",
            "applies_to_person": {
                "name": "Kelly Wrighton",
                "email": "Kelly.Wrighton@colostate.edu",
                "orcid": "orcid:0000-0003-0434-4217",
            },
        }
    ]
