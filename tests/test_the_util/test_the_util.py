from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.util import validate_json

# Tip: At the time of this writing, you can run the tests in this file without running other tests in this repo,
#      by issuing the following command from the root directory of the repository within the `fastapi` container:
#      ```
#      $ pytest tests/test_the_util/test_the_util.py
#      ```

# Define a reusable dictionary that matches the value the `validate_json` function
# returns when it considers the input to be valid.
ok_result = {"result": "All Okay!"}


def test_validate_json():
    # Get a reference to the MongoDB database, since the `validate_json` function requires
    # it to be passed in as a parameter.
    mdb = get_mongo_db()

    # Test: An empty outer dictionary is valid.
    database_dict = {}
    assert validate_json(in_docs=database_dict, mdb=mdb) == ok_result
    assert validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True) == ok_result

    # Test: An empty collection is valid.
    database_dict = {"study_set": []}
    assert validate_json(in_docs=database_dict, mdb=mdb) == ok_result
    assert validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True) == ok_result

    # Test: The function reports an error for a schema-defiant collection name.
    database_dict = {"OTHER_set": []}
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result["result"] == "errors"
    assert len(result["detail"]["OTHER_set"]) == 1
    #
    # Invoke the function-under-test again, but with referential integrity checking enabled.
    #
    result = validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True)
    assert result["result"] == "errors"
    assert len(result["detail"]["OTHER_set"]) == 1

    # Test: Two empty collections is valid.
    database_dict = {"biosample_set": [], "study_set": []}
    assert validate_json(in_docs=database_dict, mdb=mdb) == ok_result
    assert validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True) == ok_result

    # Test: A schema-compliant document is valid.
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:sty-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
            }
        ]
    }
    assert validate_json(in_docs=database_dict, mdb=mdb) == ok_result
    assert validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True) == ok_result

    # Test: A schema-defiant document is invalid.
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:OTHER-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
            },
        ]
    }
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result["result"] == "errors"
    assert len(result["detail"]["study_set"]) == 1
    #
    # Invoke the function-under-test again, but with referential integrity checking enabled.
    #
    result = validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True)
    assert result["result"] == "errors"
    assert len(result["detail"]["study_set"]) == 1

    # Test: An otherwise schema-compliant document that references a non-existent document is valid when referential
    #       integrity checking is disabled, and is invalid when referential integrity checking is enabled.
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
    assert validate_json(in_docs=database_dict, mdb=mdb) == ok_result
    #
    # Invoke the function-under-test again, but with referential integrity checking enabled.
    #
    result = validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True)
    assert len(result["detail"]["study_set"]) == 1
    assert "nmdc:sty-00-000001" in result["detail"]["study_set"][0]

    # Test: Multiple schema-compliant documents are valid.
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
    assert validate_json(in_docs=database_dict, mdb=mdb) == ok_result
    assert validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True) == ok_result

    # Test: The function reports an error for each schema-defiant document.
    database_dict = {
        "study_set": [
            {
                "id": "nmdc:OTHER-00-000001",
                "type": "nmdc:Study",
                "study_category": "research_study",
            },
            {
                "id": "nmdc:OTHER-00-000002",
                "type": "nmdc:Study",
                "study_category": "research_study",
            },
        ]
    }
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result["result"] == "errors"
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 2
    #
    # Invoke the function-under-test again, but with referential integrity checking enabled.
    #
    result = validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True)
    assert result["result"] == "errors"
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 2

    # Test: A single request can add a document that references another document added via the same request. The
    #       referential integrity checker performs its check on the _final_ result of all requested operations across
    #       all collections.
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
    assert validate_json(in_docs=database_dict, mdb=mdb, check_inter_document_references=True) == ok_result
