"""
Validates data in the EMSL collection in the nmdc_etl_staging database.
"""

from dagster import op, graph
from nmdc_runtime.site.validation.util import (
    preset_prod,
    preset_test,
    validate_mongo_collection,
)


@op
def emsl_data_object_set_collection_name():
    return "jgi.data_object_set"


@op
def emsl_omics_processing_set_collection_name():
    return "jgi.omics_processing_set"


@graph()
def emsl():
    validate_mongo_collection(emsl_data_object_set_collection_name())
    validate_mongo_collection(emsl_omics_processing_set_collection_name())


validate_emsl_job = emsl.to_job(**preset_prod)
test_validate_emsl_job = emsl.to_job(**preset_test)
