from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.util import validate_json

# Tip: At the time of this writing, you can run the tests in this file without running other tests in this repo,
#      by issuing the following command from the root directory of the repository:
#      ```
#      $ pytest tests/test_util/test_util.py
#      ```


def test_validate_json():
    # Get a reference to the MongoDB database, since the `validate_json` function requires
    # it to be passed in as a parameter.
    mdb = get_mongo_db()

    # Define a reusable dictionary that matches the value the `validate_json` function
    # returns when it considers the input to be valid.
    ok_result = {"result": "All Okay!"}

    # Test: An empty outer dictionary is valid.
    database_dict = {}
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result == ok_result

    # Test: An empty collection is valid.
    database_dict = {"study_set": []}
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result == ok_result

    # Test: Two empty collections is valid.
    database_dict = {"biosample_set": [], "study_set": []}
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result == ok_result

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
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result == ok_result

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
    result = validate_json(in_docs=database_dict, mdb=mdb)
    assert result == ok_result

    # Test: The function reports an error for the schema-defiant document.
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
    assert "study_set" in result["detail"]
    assert len(result["detail"]["study_set"]) == 1

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
