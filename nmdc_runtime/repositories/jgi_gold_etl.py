from dagster import repository

from nmdc_runtime.pipelines.jgi import gold_etl


@repository
def jgi_gold_etl():
    pipelines = [gold_etl]

    return pipelines
