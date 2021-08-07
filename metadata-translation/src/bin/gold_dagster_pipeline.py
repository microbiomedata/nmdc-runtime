#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, click, pickle
from typing import Dict, List
from git_root import git_root
import pymongo

sys.path.append(
    os.path.abspath(git_root("schema"))
)  # add path nmdc schema files and modules
sys.path.append(os.path.abspath(git_root("metadata-translation/src/bin")))
sys.path.append(os.path.abspath(git_root("metadata-translation/src/bin/lib")))

from dagster import (
    execute_pipeline,
    pipeline,
    solid,
    composite_solid,
    Failure,
    PresetDefinition,
    ModeDefinition,
    make_values_resource,
)
from lib.nmdc_etl_class import NMDC_ETL
from nmdc_schema import nmdc
import nmdc_dataframes
import pandas as pds
import json
import pkgutil
import io
from pymongo import MongoClient


@solid
def load_merged_data_source(
    context,
) -> str:
    """Create a new data source containing the merged data sources"""
    spec_file = context.solid_config["spec_file"]
    mdf = nmdc_dataframes.make_dataframe_from_spec_file(spec_file)
    print("merged data frame length:", len(mdf))

    # save merged dataframe (mdf)
    save_path = context.solid_config["save_path"]
    compression_options = dict(method="zip", archive_name=f"{save_path}")
    mdf.to_csv(
        f"{save_path}.zip", compression=compression_options, sep="\t", index=False
    )
    return save_path


@solid
def load_nmdc_etl_class(context) -> NMDC_ETL:

    # build instance of NMDC_ETL class
    etl = NMDC_ETL(
        merged_data_file=context.solid_config["data_file"],
        data_source_spec_file=context.solid_config["spec_file"],
        sssom_file="",
    )
    return etl


@solid
def transform_study(context, nmdc_etl: NMDC_ETL) -> tuple:
    # return {"study_set": nmdc_etl.transform_study()}
    return ("study_set", nmdc_etl.transform_study())


@solid
def transform_omics_processing(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("omics_processing_set", nmdc_etl.transform_omics_processing())


@solid
def transform_biosample(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("biosample_set", nmdc_etl.transform_biosample())


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
def get_mongo_db(context) -> pymongo.database.Database:
    host = context.solid_config["host"]
    username = context.solid_config["username"]
    password = context.solid_config["password"]
    dbname = context.solid_config["dbname"]

    client = MongoClient(host=host, username=username, password=password)
    return client[dbname]


@solid
def load_mongo_collection(context, mongo_db: pymongo.database.Database, data: tuple):
    collecton_name = data[0]  # get collection name
    documents = data[1]  # get data portion of tuple
    collecton = mongo_db[collecton_name]  # get mongo collection

    # drop collection if exists
    collecton.drop()

    # insert data
    collecton.insert(documents)


@pipeline
def gold_pipeline1():
    # load_merged_data_source()
    db = get_mongo_db()
    nmdc_etl = load_nmdc_etl_class()
    gold_study = transform_study(nmdc_etl)
    gold_omics_processing = transform_omics_processing(nmdc_etl)
    gold_biosample = transform_biosample(nmdc_etl)

    # load data into mongo
    load_mongo_collection(db, gold_study)
    load_mongo_collection(db, gold_omics_processing)
    load_mongo_collection(db, gold_biosample)


if __name__ == "__main__":

    # push to mongo?
    run_config1 = {
        "solids": {
            # "load_merged_data_source": {
            #     "config": {
            #         "spec_file": "lib/nmdc_data_source.yaml",
            #         "save_path": "../data/nmdc_merged_data.tsv.zip",
            #     }
            # },
            "load_nmdc_etl_class": {
                "config": {
                    "data_file": "../data/nmdc_merged_data.tsv.zip",
                    "sssom_map_file": "",
                    "spec_file": "lib/nmdc_data_source.yaml",
                }
            },
            "get_mongo_db": {
                "config": {
                    "host": os.getenv("MONGO_HOST"),
                    "username": os.getenv("MONGO_USERNAME"),
                    "password": os.getenv("MONGO_PASSWORD"),
                    "dbname": "nmdc_etl_staging",
                }
            },
        },
    }

    result = execute_pipeline(gold_pipeline1, run_config=run_config1)
