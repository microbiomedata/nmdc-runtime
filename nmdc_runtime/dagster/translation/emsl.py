"""
Translates EMSL data into JSON conformant with the NMDC JSON schema
"""
from dagster import op, graph

from nmdc_runtime.dagster.translation.util import (
    load_nmdc_etl_class,
    load_mongo_collection,
    preset_prod,
    preset_test,
)
from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL


@op
def transform_omics_processing(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("emsl.omics_processing_set", nmdc_etl.transform_emsl_omics_processing())


@op
def transform_data_object(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("emsl.data_object_set", nmdc_etl.transform_emsl_data_object())


@op
def load_mongo_collection(context, mongo_db: pymongo.database.Database, data: tuple):
    collecton_name = data[0]  # get collection name
    documents = data[1]  # get data portion of tuple
    collecton = mongo_db[collecton_name]  # get mongo collection

    # drop collection if exists
    collecton.drop()

    # insert data
    collecton.insert(documents)


@graph
def emsl():
    # load_merged_data_source()
    nmdc_etl = load_nmdc_etl_class()
    emsl_omics_processing = transform_omics_processing(nmdc_etl)
    emsl_data_object = transform_data_object(nmdc_etl)

    # load data into mongo
    load_mongo_collection(emsl_omics_processing)
    load_mongo_collection(emsl_data_object)


emsl_job = emsl.to_job(**preset_prod)
test_emsl_job = emsl.to_job(**preset_test)
