from io import BytesIO
import json
import os
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
from zipfile import ZipFile

import pymongo.database
from git_root import git_root
from dagster import (
    solid,
    AssetMaterialization,
    Output,
    EventMetadata,
    Failure,
    OutputDefinition,
    AssetKey,
)
from toolz import get_in
from pymongo import MongoClient

from nmdc_runtime.api.models.job import JobOperationMetadata
from nmdc_runtime.api.models.operation import Operation, ResultT
from nmdc_runtime.resources.core import RuntimeApiSiteClient

# resources needed for ETL
from nmdc_schema import nmdc
from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
import nmdc_runtime.lib.nmdc_dataframes as nmdc_dataframes


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
def transform_omics_processing(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("emsl.omics_processing_set", nmdc_etl.transform_emsl_omics_processing())


@solid
def transform_data_object(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("emsl.data_object_set", nmdc_etl.transform_emsl_data_object())


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


### TESTING PIPELINE
if __name__ == "__main__":
    from dagster import pipeline
    from dagster import execute_pipeline

    @pipeline
    def emsl_pipeline1():
        # load_merged_data_source()
        db = get_mongo_db()
        nmdc_etl = load_nmdc_etl_class()
        emsl_omics_processing = transform_omics_processing(nmdc_etl)
        emsl_data_object = transform_data_object(nmdc_etl)

        # load data into mongo
        load_mongo_collection(db, emsl_omics_processing)
        load_mongo_collection(db, emsl_data_object)

    run_config1 = {
        "solids": {
            "load_nmdc_etl_class": {
                "config": {
                    "data_file": git_root(
                        "metadata-translation/src/data/nmdc_merged_data.tsv.zip"
                    ),
                    "sssom_map_file": "",
                    "spec_file": git_root("nmdc_runtime/lib/nmdc_data_source.yaml"),
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

    result = execute_pipeline(emsl_pipeline1, run_config=run_config1)
