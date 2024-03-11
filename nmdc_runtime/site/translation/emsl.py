"""
Translates EMSL data into JSON conformant with the NMDC JSON schema
"""

from dagster import op, graph

from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
from nmdc_runtime.site.translation.util import (
    load_nmdc_etl_class,
    load_mongo_collection,
    preset_prod,
    preset_test,
    schema_validate,
)


@op
def transform_emsl_omics_processing(_context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("emsl.omics_processing_set", nmdc_etl.transform_emsl_omics_processing())


@op
def transform_emsl_data_object(_context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("emsl.data_object_set", nmdc_etl.transform_emsl_data_object())


@graph
def emsl():
    # load_merged_data_source()
    nmdc_etl = load_nmdc_etl_class()
    emsl_omics_processing = transform_emsl_omics_processing(nmdc_etl)
    emsl_omics_processing_validated = schema_validate(emsl_omics_processing)

    emsl_data_object = transform_emsl_data_object(nmdc_etl)
    emsl_data_object_validated = schema_validate(emsl_data_object)

    # load data into mongo
    load_mongo_collection(emsl_omics_processing_validated)
    load_mongo_collection(emsl_data_object_validated)


emsl_job = emsl.to_job(**preset_prod)
test_emsl_job = emsl.to_job(name="test_emsl", **preset_test)
