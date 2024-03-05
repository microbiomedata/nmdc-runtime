"""
Validates data in the JGI collection in the nmdc_etl_staging database.
"""

from dagster import op, graph

from nmdc_runtime.site.ops import local_file_to_api_object
from nmdc_runtime.site.validation.util import (
    preset_prod,
    preset_test,
    validate_mongo_collection,
    write_to_local_file,
    announce_validation_report,
)


@op
def jgi_data_object_set_collection_name():
    return "jgi.data_object_set"


@graph()
def jgi():
    report = validate_mongo_collection(jgi_data_object_set_collection_name())
    # the below could also be a @graph and loaded as a "subgraph" by e.g. the jgi graph job.
    local_path = write_to_local_file(report)
    obj = local_file_to_api_object(local_path)
    announce_validation_report(report, obj)


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
