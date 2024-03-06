"""
Validates data in the GOLD collection in the nmdc_etl_staging database.
"""

from dagster import op, graph
from nmdc_runtime.site.validation.util import (
    preset_prod,
    preset_test,
    validate_mongo_collection,
)


@op
def gold_study_set_collection_name():
    return "gold.study_set"


@op
def gold_omics_processing_set_collection_name():
    return "gold.omics_processing_set"


@op
def gold_biosample_set_collection_name():
    return "gold.biosample_set"


@graph()
def gold():
    validate_mongo_collection(gold_study_set_collection_name())
    validate_mongo_collection(gold_omics_processing_set_collection_name())
    validate_mongo_collection(gold_biosample_set_collection_name())


validate_gold_job = gold.to_job(**preset_prod)
test_validate_gold_job = gold.to_job(**preset_test)
