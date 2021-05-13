import jsonschema
from jsonschema import Draft7Validator
import json

## test using pytest; call with (python -m) pytest validate_json.py
# def test_always_pass():
#     assert True

# def test_always_fail():
#     assert False


def validate_json(data_path, schema_path, log_file):

    with open(data_path, "r") as json_file:  # load data
        data = json.load(json_file)

    with open(schema_path, "r") as schema_file:  # load schema
        nmdc_schema = json.load(schema_file)

    validator = Draft7Validator(nmdc_schema)
    valid = validator.is_valid(data)

    if not valid:
        with open(log_file, "w") as fp:
            for error in sorted(validator.iter_errors(data), key=lambda e: e.path):
                # print(error.message)
                fp.write(error.message)

    return valid


def test_gold_study_json(
    data_path="output/nmdc_etl/gold_study.json",
    schema_path="../../../schema/nmdc.schema.json",
    log_file="study_error.log",
):
    valid = validate_json(data_path, schema_path, log_file)

    assert valid
    return valid


if __name__ == "__main__":
    print("study test", test_gold_study_json())
