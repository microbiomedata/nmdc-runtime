import json
from functools import lru_cache
from typing import Optional

import pandas as pd
import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from toolz import dissoc

from nmdc_runtime.api.core.metadata import (
    load_changesheet,
    update_mongo_db,
    mongo_update_command_for,
    copy_docs_in_update_cmd,
    _validate_changesheet,
)
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import REPO_ROOT_DIR

TEST_DATA_DIR = REPO_ROOT_DIR.joinpath("tests", "files")


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
    sty_local_id = "sty-11-pzmd0x14"
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + sty_local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{sty_local_id}.json")
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv"), mdb
    )
    assert isinstance(df, pd.DataFrame)
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + sty_local_id})


def test_changesheet_update_slot_with_range_decimal():
    mdb = get_mongo_db()
    bsm_local_id = "bsm-11-0pyv7738"
    remove_tmp_doc = False
    if mdb.biosample_set.find_one({"id": "nmdc:" + bsm_local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{bsm_local_id}.json")
        ) as f:
            mdb.biosample_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        REPO_ROOT_DIR.joinpath("tests", "files", "test_changesheet_decimal_value.tsv"),
        mdb,
    )
    _validate_changesheet(df, mdb)
    if remove_tmp_doc:
        mdb.biosample_set.delete_one({"id": "nmdc:" + bsm_local_id})


def test_changesheet_update_slot_with_range_bytes():
    mdb = get_mongo_db()
    dobj_local_id = "dobj-11-000n1286"
    remove_tmp_doc = False
    if mdb.data_object_set.find_one({"id": "nmdc:" + dobj_local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{dobj_local_id}.json")
        ) as f:
            mdb.data_object_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        REPO_ROOT_DIR.joinpath(
            "tests", "files", "test_changesheet_update_bytes_ranged_slot.tsv"
        ),
        mdb,
    )
    _validate_changesheet(df, mdb)
    if remove_tmp_doc:
        mdb.data_object_set.delete_one({"id": "nmdc:" + dobj_local_id})


def test_changesheet_update_slot_with_range_uriorcurie():
    mdb = get_mongo_db()
    local_id = "sty-11-pzmd0x14"
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{local_id}.json")
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        REPO_ROOT_DIR.joinpath(
            "tests", "files", "test_changesheet_insert_study_doi.tsv"
        ),
        mdb,
    )
    _validate_changesheet(df, mdb)
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + local_id})


@pytest.mark.parametrize(
    "seeded_db", ["docs_for_seeded_db_for_changesheet_study_update"], indirect=True
)
def test_changesheet_study_update(
    docs_for_seeded_db_for_changesheet_study_update, seeded_db
):
    mdb = get_mongo(run_config_frozen__normal_env).db
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")

    pi_info = {
        "name": "NEW PI NAME 1",
        "has_raw_value": "NEW RAW NAME 1",
        "type": "nmdc:PersonValue",
    }
    assert study_doc.get("principal_investigator") != pi_info
    assert study_doc.get("name") != "NEW STUDY NAME 1"
    assert study_doc.get("ecosystem") != "NEW ECOSYSTEM 1"
    assert study_doc.get("ecosystem_type") != "NEW ECOSYSTEM_TYPE 1"
    assert study_doc.get("ecosystem_subtype") != "NEW ECOSYSTEM_SUBTYPE 1"

    website_info = ["HTTP://TEST1.EXAMPLE.COM", "HTTP://TEST2.EXAMPLE.COM"]
    for website in website_info:
        assert website not in study_doc.get("websites", [])

    update_cmd = mongo_update_command_for(df)
    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_result = results[0]
    assert first_result["doc_after"]["principal_investigator"] == pi_info
    assert first_result["doc_after"]["name"] == "NEW STUDY NAME 1"
    assert first_result["doc_after"]["ecosystem"] == "NEW ECOSYSTEM 1"
    assert first_result["doc_after"]["ecosystem_type"] == "NEW ECOSYSTEM_TYPE 1"
    assert first_result["doc_after"]["ecosystem_subtype"] == "NEW ECOSYSTEM_SUBTYPE 1"
    for website in website_info:
        assert website in first_result["doc_after"]["websites"]
    assert first_result["validation_errors"] == []


def test_changesheet_array_item_nested_attributes():
    mdb = get_mongo(run_config_frozen__normal_env).db
    local_id = "sty-11-r2h77870"
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath(
                "tests", "files", f"study_no_credit_associations.json"
            )
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-array-item-nested-attributes.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")

    credit_info = {
        "applied_roles": ["Conceptualization"],
        "applies_to_person": {
            "name": "CREDIT NAME 1",
            "email": "CREDIT_NAME_1@foo.edu",
            "orcid": "orcid:0000-0000-0000-0001",
        },
    }
    assert credit_info not in study_doc.get("has_credit_associations", [])

    update_cmd = mongo_update_command_for(df)
    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_doc_after = results[0]["doc_after"]
    assert "has_credit_associations" in first_doc_after
    assert credit_info in first_doc_after.get("has_credit_associations", [])
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + local_id})


def test_update_pi_websites():
    mdb = get_mongo(run_config_frozen__normal_env).db
    local_id = "sty-11-r2h77870"
    restore_original_doc = False
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath(
                "tests", "files", f"study_no_credit_associations.json"
            )
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    else:
        restore_original_doc = True
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-update-pi-websites.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")

    pi_info = {
        "has_raw_value": "NEW PI NAME",
        "name": "NEW PI NAME",
        "profile_image_url": "https://portal.nersc.gov/NEW-PI-NAME.jpg",
        "orcid": "orcid:0000-0000-0000-0000",
        "websites": ["https://www.ornl.gov/staff-profile/NEW-PI-NAME"],
    }
    assert study_doc.get("principal_investigator", []) != pi_info

    update_cmd = mongo_update_command_for(df)

    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_result_pi_info = results[0]["doc_after"]["principal_investigator"]
    for k, v in pi_info.items():
        assert first_result_pi_info[k] == v
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + local_id})
    if restore_original_doc:
        mdb.study_set.replace_one({"id": id_}, study_doc)


def test_update_biosample_ph():
    mdb = get_mongo_db()
    doc = json.loads(
        (REPO_ROOT_DIR / "tests" / "files" / "nmdc_bsm-11-5nhz3402.json").read_text()
    )
    mdb.biosample_set.replace_one({"id": "nmdc:bsm-11-5nhz3402"}, doc, upsert=True)
    df = load_changesheet(
        (REPO_ROOT_DIR / "tests" / "files" / "test_changesheet_update_one_ph.tsv"), mdb
    )

    update_cmd = mongo_update_command_for(df)

    assert isinstance(
        update_cmd["nmdc:bsm-11-5nhz3402"]["updates"][0]["u"]["$set"]["ph"], float
    )


def test_update_mongo_db_returns_error_for_missing_document():
    """
    Regression test for the bug where update_mongo_db raised
    "TypeError: object of type 'NoneType' has no len()" when the changesheet
    referenced a document ID that did not exist in the target database.

    Root cause: pymongo's find_one() returns None (not a dict) when no matching
    document is found. The code was passing that None directly to toolz's dissoc(),
    which calls len() on its first argument and therefore crashes on None.

    This test constructs an update_cmd that references a document ID that is
    deliberately absent from the database, then asserts that update_mongo_db
    does NOT raise an exception and instead returns a result entry that contains
    a descriptive validation error message explaining the document is missing.
    """
    mdb = get_mongo_db()

    # Choose a document ID that is guaranteed not to be in the database for this test.
    # We use a synthetic ID that would never be minted in production.
    nonexistent_id = "nmdc:bsm-00-doesnotexist"

    # Ensure the document truly does not exist before we start (clean up any
    # leftover state from a previously interrupted test run).
    mdb.biosample_set.delete_one({"id": nonexistent_id})

    # Build an update_cmd that mimics what mongo_update_command_for() would produce
    # for a changesheet row targeting the nonexistent document. The structure must
    # match what update_mongo_db() expects: a dict keyed by document ID, with each
    # value being a dict containing at least "update" (collection name) and "updates"
    # (list of MongoDB update sub-commands).
    update_cmd = {
        nonexistent_id: {
            "update": "biosample_set",
            "updates": [
                {
                    "q": {"id": nonexistent_id},
                    "u": {"$set": {"name": "should not matter"}},
                    "multi": False,
                    "upsert": False,
                }
            ],
        }
    }

    # Before the fix this call would raise:
    #   TypeError: object of type 'NoneType' has no len()
    # because find_one() returned None for the missing document and that None was
    # passed directly to toolz.dissoc().
    results = update_mongo_db(mdb, update_cmd)

    # The function should return one result entry per document ID in update_cmd.
    assert len(results) == 1

    result = results[0]

    # The result should record the ID that was processed.
    assert result["id"] == nonexistent_id

    # Because the document did not exist, doc_before and doc_after should be None
    # (the function had no document to read or update).
    assert result["doc_before"] is None
    assert result["doc_after"] is None

    # The result should include a validation error describing the missing document,
    # so callers can surface a useful message to the user instead of a 500 error.
    assert len(result["validation_errors"]) > 0
    assert nonexistent_id in result["validation_errors"][0]
