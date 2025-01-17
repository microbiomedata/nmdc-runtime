import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.util import validate_json

# Tip: At the time of this writing, you can run the tests in this file without running other tests in this repo,
#      by issuing the following command from the root directory of the repository within the `fastapi` container:
#      ```
#      $ pytest -vv tests/test_the_util/test_the_util.py
#      ```

# Define a reusable dictionary that matches the value the `validate_json` function
# returns when it considers the input to be valid.
ok_result = {"result": "All Okay!"}

# Make a concise alias whose items we can unpack (via `**check_refs`) into `kwargs`
# when invoking the `validate_json` function in our tests.
check_refs = dict(check_inter_document_references=True)


@pytest.fixture
def db():
    r"""Returns a reference to the MongoDB database specified by environment variables."""
    return get_mongo_db()


def test_validate_json_returns_valid_when_input_is_empty_dictionary(db):
    assert validate_json({}, mdb=db) == ok_result
    assert validate_json({}, mdb=db, **check_refs) == ok_result


def test_validate_json_returns_valid_when_collection_is_empty_list(db):
    database_dict = {"study_set": []}
    assert validate_json(in_docs=database_dict, mdb=db) == ok_result
    assert validate_json(in_docs=database_dict, mdb=db, **check_refs) == ok_result


def test_validate_json_returns_invalid_when_collection_name_is_schema_defiant(db):
    database_dict = {"OTHER_set": []}
    result = validate_json(in_docs=database_dict, mdb=db)
    assert result["result"] == "errors"
    assert len(result["detail"]["OTHER_set"]) == 1

    # Invoke the function-under-test again, but with referential integrity checking enabled.
    result = validate_json(in_docs=database_dict, mdb=db, **check_refs)
    assert result["result"] == "errors"
    assert len(result["detail"]["OTHER_set"]) == 1


def test_validate_json_returns_valid_when_payload_has_multiple_empty_collections(db):
    database_dict = {"biosample_set": [], "study_set": []}
    assert validate_json(in_docs=database_dict, mdb=db) == ok_result
    assert validate_json(in_docs=database_dict, mdb=db, **check_refs) == ok_result


def test_validate_json_returns_valid_when_the_only_document_is_schema_compliant(db):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
            }
        ]
    }
    assert validate_json(in_docs=database_dict, mdb=db) == ok_result
    assert validate_json(in_docs=database_dict, mdb=db, **check_refs) == ok_result


def test_validate_json_returns_valid_when_all_documents_are_schema_compliant(db):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
            },
            {
                "id": "nmdc:sty-00-000002",
                "type": "nmdc:Study",
                "study_category": "research_study",
            },
        ]
    }
    assert validate_json(in_docs=database_dict, mdb=db) == ok_result
    assert validate_json(in_docs=database_dict, mdb=db, **check_refs) == ok_result


def test_validate_json_returns_invalid_when_document_is_schema_defiant(db):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:OTHER-00-000001",  # invalid string format
                "type": "nmdc:Study",
                "study_category": "research_study",
            },
        ]
    }
    result = validate_json(in_docs=database_dict, mdb=db)
    assert result["result"] == "errors"
    assert len(result["detail"]["study_set"]) == 1

    # Invoke the function-under-test again, but with referential integrity checking enabled.
    result = validate_json(in_docs=database_dict, mdb=db, **check_refs)
    assert result["result"] == "errors"
    assert len(result["detail"]["study_set"]) == 1


def test_validate_json_returns_invalid_when_otherwise_schema_compliant_document_references_missing_document(db):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
                "part_of": ["nmdc:sty-00-999999"],  # identifies a non-existent study
            }
        ]
    }
    assert validate_json(in_docs=database_dict, mdb=db) == ok_result

    # Invoke the function-under-test again, but with referential integrity checking enabled.
    result = validate_json(in_docs=database_dict, mdb=db, **check_refs)
    assert len(result["detail"]["study_set"]) == 1
    assert "nmdc:sty-00-000001" in result["detail"]["study_set"][0]


def test_validate_json_does_not_check_references_if_documents_are_schema_defiant(db):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:OTHER-00-000001",  # invalid string format
                "type": "nmdc:Study",
                "study_category": "research_study",
                "part_of": ["nmdc:sty-00-000009"],  # identifies a non-existent study
            },
        ]
    }
    result = validate_json(in_docs=database_dict, mdb=db)
    assert result["result"] == "errors"
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 1

    # Invoke the function-under-test again, but with referential integrity checking enabled.
    result = validate_json(in_docs=database_dict, mdb=db, **check_refs)
    assert result["result"] == "errors"
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 1  # not 2


def test_validate_json_reports_multiple_broken_references_emanating_from_single_document(db):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
                "part_of": ["nmdc:sty-00-000008", "nmdc:sty-00-000009"],  # identifies 2 non-existent studies
            },
        ]
    }
    result = validate_json(in_docs=database_dict, mdb=db, **check_refs)
    assert result["result"] == "errors"
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 2


def test_validate_json_checks_referential_integrity_after_applying_all_collections_changes(db):
    r"""
    Note: This test targets the scenario where a single payload introduces both the source document and target document
          of a given reference, and those documents reside in different collections. If the referential integrity
          checker were to performs a check after each individual collection's changes had been applied, it would not
          find referenced documents that hadn't been introduced into the database yet.
    """
    database_dict = {
        "biosample_set": [
            {
                "id": "nmdc:bsm-00-000001",
                "type": "nmdc:Biosample",
                "associated_studies": ["nmdc:sty-00-000001"],
                "env_broad_scale": {"term": {"type": "nmdc:OntologyClass", "id": "ENVO:000000"}, "type": "nmdc:ControlledIdentifiedTermValue"}, "env_local_scale": {"term": {"type": "nmdc:OntologyClass", "id": "ENVO:000000"}, "type": "nmdc:ControlledIdentifiedTermValue"}, "env_medium": {"term": {"type": "nmdc:OntologyClass", "id": "ENVO:000000"}, "type": "nmdc:ControlledIdentifiedTermValue"}
            }
        ],
        "study_set": [
            {"id": "nmdc:sty-00-000001", "type": "nmdc:Study", "study_category": "research_study"},
            {"id": "nmdc:sty-00-000002", "type": "nmdc:Study", "study_category": "research_study", "part_of": ["nmdc:sty-00-000001"]}
        ]
    }
    assert validate_json(in_docs=database_dict, mdb=db, **check_refs) == ok_result


def test_validate_json_considers_existing_documents_when_checking_references(db):
    r"""
    Note: This test focuses on the case where the database already contains the to-be-referenced document;
          as opposed to the to-be-referenced document being introduced via the same request payload.
          For that reason, we will seed the database before calling the function-under-test.
    """
    existing_study_id = "nmdc:sty-00-000001"

    db.get_collection("study_set").replace_one(
        {"id": existing_study_id},
        {"id": existing_study_id, "type": "nmdc:Study", "study_category": "research_study"},
        upsert=True
    )
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000002",
                "type": "nmdc:Study",
                "study_category": "research_study",
                "part_of": [existing_study_id],  # identifies the existing study
            },
        ]
    }
    assert validate_json(in_docs=database_dict, mdb=db, **check_refs) == ok_result

    db.get_collection("study_set").delete_one({"id": existing_study_id})
