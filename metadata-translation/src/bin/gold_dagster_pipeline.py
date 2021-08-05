#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, click, pickle
from typing import Dict, List
from git_root import git_root

sys.path.append(
    os.path.abspath(git_root("schema"))
)  # add path nmdc schema files and modules
sys.path.append(os.path.abspath(git_root("metadata-translation/src/bin")))
sys.path.append(os.path.abspath(git_root("metadata-translation/src/bin/lib")))

from dagster import execute_pipeline, pipeline, solid, composite_solid, Failure
from lib.nmdc_etl_class import NMDC_ETL
from nmdc_schema import nmdc
import nmdc_dataframes
import pandas as pds
import json
import pkgutil
import io


@solid
def load_merged_data_source(
    context,
) -> str:
    """Create a new data source containing the merged data sources"""
    mdf = nmdc_dataframes.make_dataframe_from_spec_file(
        context.solid_config["spec_file"]
    )  # build merged data frame (mdf)

    print("merged data frame length:", len(mdf))

    # save merged dataframe (mdf)
    save_path = context.solid_config["save_path"]
    compression_options = dict(method="zip", archive_name=f"{save_path}")
    mdf.to_csv(
        f"{save_path}.zip", compression=compression_options, sep="\t", index=False
    )
    return save_path


@solid
def load_nmdc_etl_class(context, data_file) -> NMDC_ETL:

    # build instance of NMDC_ETL class
    etl = NMDC_ETL(
        merged_data_file=data_file,
        data_source_spec_file=context.solid_config["spec_file"],
        sssom_file="",
    )
    return etl


@solid
def load_gold_study(conext, nmdc_etl_class: NMDC_ETL):
    return nmdc_etl_class.study


@solid
def transform_study(context, nmdc_etl_class: NMDC_ETL) -> list:
    study = nmdc_etl_class.transform_study()
    return study


@solid
def make_nmdc_database(context, gold_study):
    database = {"study_set": [*gold_study]}


@solid(required_resource_keys={"mongo"})
def transform_set_and_load(context, nmdc_etl_class, set_name):
    objects = getattr(mdc_etl_class, f"transform_{set_name}")()
    mdb: pymongo.database.Database = context.resources.mongo.db
    mdb[f"{set_name}_set"].insert_many(objects)


# database = {
#     "study_set": [*gold_study],
#     "omics_processing_set": [*gold_project, *emsl_project],
#     "biosample_set": [*gold_biosample],
#     "data_object_set": [
#         *jgi_data_object,
#         *emsl_data_object,
#         *mg_assembly_data_objects,
#         *readQC_data_objects,
#     ],
#     "metagenome_assembly_set": [*mg_assembly_activities],
#     "read_QC_analysis_activity_set": [*readQC_activities],
# }


@solid
def load_mongo_collection(context, collection, data):
    pass


@pipeline
def study_pipeline1():
    etl = load_nmdc_etl_class(load_merged_data_source())
    gold_study = transform_study(etl)

    ## workflows for the rest will be similary
    # gold_project = transfrom_project(etl)
    # biosample = ...
    # ...
    make_nmdc_database(gold_study)

    # push to mongo?


if __name__ == "__main__":

    run_config2 = {
        "solids": {
            "load_merged_data_source": {
                "config": {
                    "spec_file": "lib/nmdc_data_source.yaml",
                    "save_path": "../data/nmdc_merged_data.tsv.zip",
                }
            },
            "load_nmdc_etl_class": {
                "config": {
                    "data_file": "../data/nmdc_merged_data.tsv.zip",
                    "sssom_map_file": "",
                    "spec_file": "lib/nmdc_data_source.yaml",
                }
            },
        },
        "resources": {
            "mongo": {
                "config": {
                    "host": {"env": "MONGO_HOST"},
                    "username": {"env": "MONGO_USERNAME"},
                    "password": {"env": "MONGO_PASSWORD"},
                    "dbname": {"env": "MONGO_DBNAME"},
                },
            },
        },
    }

    result = execute_pipeline(study_pipeline1, run_config=run_config2)
    # print(result)

    run_config1 = {
        "solids": {
            "load_merged_data_source": {
                "config": {
                    "spec_file": "lib/nmdc_data_source.yaml",
                    "save_path": "../data/nmdc_merged_data.tsv",
                }
            }
        }
    }

    run_config3 = {
        "solids": {
            "load_merged_data_source": {
                "config": {
                    "spec_file": "lib/nmdc_data_source.yaml",
                    "save_path": "../data/nmdc_merged_data.tsv",
                },
                "solids": {
                    "load_nmdc_etl_class": {
                        "config": {
                            "data_file": "../data/nmdc_merged_data.tsv.zip",
                            "sssom_map_file": "",
                            "spec_file": "lib/nmdc_data_source.yaml",
                        }
                    }
                },
            }
        }
    }

    load_merged_data_source_dict = {
        "load_merged_data_source": {
            "config": {
                "spec_file": "lib/nmdc_data_source.yaml",
                "save_path": "../data/nmdc_merged_data.tsv",
            }
        }
    }

    load_nmdc_etl_class_dict = {
        "load_nmdc_etl_class": {
            "config": {
                "data_file": "../data/nmdc_merged_data.tsv.zip",
                "sssom_map_file": "",
                "spec_file": "lib/nmdc_data_source.yaml",
            }
        }
    }

    test1 = {"solids": [load_merged_data_source_dict, load_nmdc_etl_class_dict]}

    config1 = {
        "config": {
            "spec_file": "lib/nmdc_data_source.yaml",
            "save_path": "../data/nmdc_merged_data.tsv",
        }
    }

    config2 = {
        "config": {
            "data_file": "../data/nmdc_merged_data.tsv.zip",
            "sssom_map_file": "",
            "spec_file": "lib/nmdc_data_source.yaml",
        }
    }
    test2 = {
        "solids": {"load_merged_data_source": config1, "load_nmdc_etl_class": config2}
    }

    test3 = {"solids": {"load_merged_data_source": config1}}
    test4 = {"solids": load_nmdc_etl_class_dict}
