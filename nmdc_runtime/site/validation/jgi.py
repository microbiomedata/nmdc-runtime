"""
Validates data in the JGI collection in the nmdc_etl_staging database.
"""
from dagster import op, graph
from nmdc_runtime.site.validation.util import (
    preset_prod,
    preset_test,
    validate_mongo_collection,
)


@op
def jgi_data_object_set_collection_name():
    return "jgi.data_object_set"


@graph()
def jgi():
    validate_mongo_collection(jgi_data_object_set_collection_name())


# passing the collecton name via the config
# problem: not sure if this best when multiple sets need to be validated
# from toolz import assoc_in
# config_ops = {
#     "validate_mongo_collection": {"config": {"collection_name": "jgi.data_object_set"}}
# }
# validate_jgi_job = jgi.to_job(**assoc_in(preset_prod, ["config", "ops"], config_ops))
# test_validate_jgi_job = jgi.to_job(
#     **assoc_in(preset_test, ["config", "ops"], config_ops)
# )

validate_jgi_job = jgi.to_job(**preset_prod)
test_validate_jgi_job = jgi.to_job(**preset_test)
