import sys

from jsonschema import Draft7Validator
import json


def validate_json(data_path: str, schema_path: str, log_file: str) -> bool:
    r"""
    TODO: Document this function.
    """

    with open(data_path, "r") as json_file:  # load data
        data = json.load(json_file)

    with open(schema_path, "r") as schema_file:  # load schema
        nmdc_schema = json.load(schema_file)

    validator = Draft7Validator(nmdc_schema)
    valid = validator.is_valid(data)

    if not valid:
        with open(log_file, "w") as fp:
            for error in sorted(validator.iter_errors(data), key=lambda e: e.path):
                fp.write(error.message)

    return valid


def test_gold_study_json(
    schema_path: str,
    data_path: str,
    log_file_path: str = "error.log",
) -> bool:
    r"""
    Validates the specified data against the specified schema, writing any validation errors to the specified log file.

    :param schema_path:   Path to JSON-formatted NMDC Schema file against which you want to validate the data.
                          Example value: `/path/to/nmdc_materialized_patterns.schema.json`
    :param data_path:     Path to JSON-formatted data file you want to validate.
                          Example value: `/path/to/nmdc_etl/gold_study.json`
    :param log_file_path: Path to log file to which you want the function to write validation error messages.
    """
    valid = validate_json(data_path, schema_path, log_file_path)

    assert valid
    return valid


if __name__ == "__main__":
    r"""
    Note: In 2025, this script was updated ("quick 'n dirty"-ly) to allow the user to specify the various file paths via
          CLI arguments. That update was prompted by team members noticing the hard-coded file paths in this script were
          obsolete (i.e. they were paths to files that no longer existed in the repository).
    """

    # If an invalid number of CLI arguments was specified, abort and display a usage string.
    if len(sys.argv) < 3:
        raise SystemExit("Usage: script.py SCHEMA_PATH DATA_PATH [LOG_FILE_PATH]")

    print("study test", test_gold_study_json(
        schema_path=sys.argv[1],
        data_path=sys.argv[2],
        log_file_path=sys.argv[3] if len(sys.argv) == 4 else None,
    ))
