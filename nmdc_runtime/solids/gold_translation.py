"""
Translating an export of the JGI GOLD [1] database of SQL tables to the NMDC database JSON schema.

[1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
"""

from io import BytesIO
import json
import os
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
from zipfile import ZipFile

import pymongo.database
from git_root import git_root
from dagster import solid
from pymongo import MongoClient

from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL


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

    result = execute_pipeline(gold_pipeline1, run_config=run_config1)
