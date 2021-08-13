## author: Bill Duncan
## summary: Contains methods for saving or loading NMDC data into a resource.

import json
import jq


def save_json(json_data, file_path: str):
    ## save json with changed data types
    with open(file_path, "w") as out_file:
        json.dump(json_data, out_file, indent=2)
    return json_data


def get_json_from_file(file_path: str, replace_single_quote=False):
    ## load json
    with open(file_path, "r") as in_file:
        if replace_single_quote:  # json
            text = in_file.read()
            json_data = json.loads(text.replace("'", '"'))
        else:
            json_data = json.load(in_file)
    return json_data


def get_json(file_path="", replace_single_quote=False):
    if len(file_path) > 0:
        return get_json_from_file(file_path, replace_single_quote)


def save_nmdc_dict_as_json_to_file(nmdc_dict: dict, file_path: str):
    with open(file_path, "w") as f:
        json.dump(nmdc_dict, f, indent=2)
    return json.dumps(nmdc_dict, indent=2)


def save_nmdc_dict(nmdc_dict: dict, file_path="", data_format="json"):
    if len(file_path) > 0:
        if "json" == data_format:
            return save_nmdc_dict_as_json_to_file(nmdc_dict, file_path)


def make_nmdc_example_database(
    gold_study_file="output/nmdc_etl/gold_study.json",
    gold_omics_processing_file="output/nmdc_etl/gold_omics_processing.json",
    gold_biosample_file="output/nmdc_etl/gold_biosample.json",
    jgi_fastq_data_object_file="output/nmdc_etl/jgi_fastq_data_objects.json",
    output_file="output/nmdc_example-database.json",
):
    ## load json files
    biosample_json = get_json(gold_biosample_file)
    projects_json = get_json(gold_omics_processing_file)
    study_json = get_json(gold_study_file)
    data_objects_json = get_json(jgi_fastq_data_object_file)

    ## get a list of distinct omics processing study ids, and choose the first 3 studies
    study_ids = set(
        jq.compile(".[] | .part_of[]").input(projects_json).all()
    )  # all returns a list
    study_ids = list(study_ids)[0:3]
    # study_ids =

    ## build a test set of studies from the study ids
    study_test = (
        jq.compile(
            ".[] | select( .id == ("
            + ", ".join('"{0}"'.format(id) for id in study_ids)
            + "))"
        )
        .input(study_json)
        .all()
    )  # all() returns a list

    ## build a test set of projects from the study ids
    ## note: the jq query only selects first omics found for a given study id
    projects_test = []
    for id in study_ids:
        j = (
            jq.compile(f'[.[] | select( .part_of[]? |  . == "{id}")][0]')
            .input(projects_json)
            .all()
        )
        projects_test.append(*j)

    ## get list of unique biossample ids from omics processing and build biosample test set
    biosample_ids = (
        jq.compile(".[] | .has_input[]?").input(projects_test).all()
    )  # all() returns a list
    biosample_test = (
        jq.compile(
            ".[] | select( .id == ("
            + ", ".join('"{0}"'.format(id) for id in biosample_ids)
            + "))"
        )
        .input(biosample_json)
        .all()
    )  # all() returns a list

    ## get a list of data object ids and build data objects test set
    data_objects_ids = (
        jq.compile(".[] | .has_output[]?").input(projects_test).all()
    )  # all() returns a list
    data_objects_test = (
        jq.compile(
            ".[] | select( .id == ("
            + ", ".join('"{0}"'.format(id) for id in data_objects_ids)
            + "))"
        )
        .input(data_objects_json)
        .all()
    )  # all() returns a list

    ## compile into database object
    database = {
        "study_set": [*study_test],
        "omics_processing_set": [*projects_test],
        "biosample_set": [*biosample_test],
        "data_object_set": [*data_objects_test],
    }

    save_json(database, output_file)
