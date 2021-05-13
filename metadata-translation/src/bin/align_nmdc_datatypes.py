import os, sys

sys.path.append(
    os.path.abspath("../../../schema")
)  # add path nmdc schema files and modules

import yaml
import json
from yaml import CLoader as Loader, CDumper as Dumper
from collections import namedtuple
from pprint import pprint
import pandas as pds
import jsonasobj
from nmdc import (
    Biosample,
    GeolocationValue,
    ControlledTermValue,
    QuantityValue,
    Activity,
    OntologyClass,
)
import lib.data_operations as dop

# TODO: convert triad value in complex object; convert depth the measurement unit ("meters")


def get_json(file_path):
    ## load json
    with open(file_path, "r") as in_file:
        json_list = json.load(in_file)
    return json_list


def save_json(json_list, file_path):
    ## save json with changed data types
    with open(file_path, "w") as out_file:
        json.dump(json_list, out_file, indent=2)
    return json_list


def convert_env_triad(attribute_value):
    ## replace '_' with ':' in curie
    curie_val = attribute_value_to_string(attribute_value)
    curie_val = curie_val.replace("_", ":")

    obj = ControlledTermValue(has_raw_value=curie_val, term=OntologyClass(id=curie_val))
    return json.loads(jsonasobj.as_json(obj))


def attribute_value_to_QuantityValue(attribute_value, unit):
    val = f"{attribute_value['has_raw_value']} {unit}"
    obj = QuantityValue(val)
    return json.loads(jsonasobj.as_json(obj))


def attribute_value_to_datatype(attribute_value, data_type="string", unit=""):
    val = attribute_value["has_raw_value"]
    if data_type == "integer":
        return int(val)
    elif data_type == "float":
        return float(val)
    elif data_type == "quantity value":
        return attribute_value_to_QuantityValue(attribute_value, unit)
    else:
        return str(val)


def attribute_value_to_int(attribute_value):
    return attribute_value_to_datatype(attribute_value, "integer")


def attribute_value_to_string(attribute_value):
    return attribute_value_to_datatype(attribute_value, "string")


def align_data_object(file_path):
    json_list = get_json(file_path)  # load json

    ## change file size bytes to int
    for item in json_list:
        item["file_size_bytes"]["has_raw_value"] = attribute_value_to_int(
            item["file_size_bytes"]
        )

    save_json(json_list, file_path)  # save json


def align_emsl_data_object():
    align_data_object("output/nmdc_etl/emsl_data_objects.json")


def align_jgi_data_object():
    align_data_object("output/nmdc_etl/jgi_fastq_data_objects.json")


def align_biosample(file_path):
    json_list = get_json(file_path)  # load json

    ## change file size bytes to int
    for item in json_list:
        item["env_broad_scale"] = convert_env_triad(item["env_broad_scale"])
        item["env_local_scale"] = convert_env_triad(item["env_local_scale"])
        item["env_medium"] = convert_env_triad(item["env_medium"])
        if "depth" in item.keys():
            item["depth"] = attribute_value_to_datatype(
                item["depth"], "quantity value", "m"
            )

    save_json(json_list, file_path)  # save json


def align_gold_biosample():
    align_biosample("output/nmdc_etl/gold_biosample.json")


def main():
    pass


if __name__ == "__main__":
    main()
