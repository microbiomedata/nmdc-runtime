"""
Tip: At the time of this writing, you can run the tests in this file without running other tests in this repo,
by issuing the following command from the root directory of the repository within the `fastapi` container:

    ```
    $ pytest -vv tests/test_the_util/test_the_util.py
    ```
"""

import doctest

import pytest
from refscan.lib.Finder import Finder
from refscan.scanner import scan_outgoing_references

from nmdc_runtime.api.db.mongo import get_mongo_db, validate_json
from nmdc_runtime.api.endpoints.lib import path_segments
from nmdc_runtime.util import (
    decorate_if,
    get_allowed_references,
    nmdc_schema_view,
)
from tests.lib.faker import Faker

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


def test_validate_json_returns_invalid_when_otherwise_schema_compliant_document_references_missing_document(
    db,
):
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


def test_validate_json_reports_multiple_broken_references_emanating_from_single_document(
    db,
):
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
                "part_of": [
                    "nmdc:sty-00-000008",
                    "nmdc:sty-00-000009",
                ],  # identifies 2 non-existent studies
            },
        ]
    }
    result = validate_json(in_docs=database_dict, mdb=db, **check_refs)
    assert result["result"] == "errors"
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 2


def test_validate_json_checks_referential_integrity_after_applying_all_collections_changes(
    db,
):
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
                "env_broad_scale": {
                    "term": {"type": "nmdc:OntologyClass", "id": "ENVO:000000"},
                    "type": "nmdc:ControlledIdentifiedTermValue",
                },
                "env_local_scale": {
                    "term": {"type": "nmdc:OntologyClass", "id": "ENVO:000000"},
                    "type": "nmdc:ControlledIdentifiedTermValue",
                },
                "env_medium": {
                    "term": {"type": "nmdc:OntologyClass", "id": "ENVO:000000"},
                    "type": "nmdc:ControlledIdentifiedTermValue",
                },
            }
        ],
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
                "part_of": ["nmdc:sty-00-000001"],
            },
        ],
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
        {
            "id": existing_study_id,
            "type": "nmdc:Study",
            "study_category": "research_study",
        },
        upsert=True,
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


def test_referential_integrity_checker_supports_pending_mongo_transactions(db):
    r"""
    Note: This test was written to demonstrate how developers can validate the
          referential integrity of the result of a pending Mongo transaction
          without having to commit that transaction.
    """

    # Setup the referential integrity checker.
    references = get_allowed_references()

    # Seed the database with two studies, one of which references the other.
    faker = Faker()
    study_a, study_b = faker.generate_studies(2)
    study_set = db.get_collection("study_set")
    study_ids = [study_a["id"], study_b["id"]]
    study_a["part_of"] = [study_b["id"]]  # link the two studies
    assert study_set.count_documents({"id": {"$in": study_ids}}) == 0
    study_set.insert_many([study_a, study_b])

    # Validate referential integrity of the _referencing_ study (this is baseline behavior).
    violations = scan_outgoing_references(
        document=study_a,
        schema_view=nmdc_schema_view(),
        references=references,
        finder=Finder(database=db),
        source_collection_name="study_set",
    )
    assert len(violations) == 0

    # Start a Mongo transaction.
    with db.client.start_session() as session:
        with session.start_transaction():
            # Stage a change that, if committed, would introduce a broken reference.
            # Note: In this case, we'll stage the deletion of the _referenced_ study.
            study_set.delete_one({"id": study_b["id"]}, session=session)
            assert (
                study_set.count_documents({"id": study_a["id"]}, session=session) == 1
            )
            assert (
                study_set.count_documents({"id": study_b["id"]}, session=session) == 0
            )  # <-- deleted

            # Now, we'll validate the referential integrity of the other study.
            # Note: We expect this to detect the broken reference, even though
            #       the delete operation has not been _committed_ yet.
            violations = scan_outgoing_references(
                document=study_a,
                schema_view=nmdc_schema_view(),
                references=references,
                finder=Finder(database=db),
                source_collection_name="study_set",
                client_session=session,  # so the scan happens within the context of this session
            )
            assert len(violations) == 1

            # Abort the transaction since there were violations.
            session.abort_transaction()

    # Finally, confirm the delete operation was not committed.
    assert study_set.count_documents({"id": study_a["id"]}) == 1
    assert study_set.count_documents({"id": study_b["id"]}) == 1  # <-- not deleted

    # 🧹 Clean up.
    study_set.delete_many({"id": {"$in": study_ids}})


def test_path_segments():
    """Test all doctests in `nmdc_runtime.api.endpoints.lib.path_segments`."""
    failure_count, test_count = doctest.testmod(path_segments, verbose=True)
    assert failure_count == 0, f"{failure_count} doctests failed out of {test_count}"


def test_decorate_if():
    """Demonstrates usages of the `@decorate_if` decorator."""

    def parenthesize(func):
        """Decorator that wraps the function's output in parentheses."""

        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            return f"({result})"

        return wrapper

    @parenthesize  # regular decoration
    def get_apple() -> str:
        return "apple"

    @decorate_if()(parenthesize)  # condition defaults to `False`
    def get_banana() -> str:
        return "banana"

    @decorate_if(True)(parenthesize)
    def get_carrot() -> str:
        return "carrot"

    @decorate_if(False)(parenthesize)
    def get_daikon() -> str:
        return "daikon"

    assert get_apple() == "(apple)"
    assert get_banana() == "banana"
    assert get_carrot() == "(carrot)"
    assert get_daikon() == "daikon"
