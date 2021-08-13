from dagster import graph

from nmdc_runtime.dagster.ops import (
    build_merged_db,
    run_etl,
    local_file_to_api_object,
    get_operation,
    produce_curated_db,
    delete_operations,
    create_objects_from_ops,
    list_operations,
    filter_ops_done_object_puts,
    hello,
    mongo_stats,
    update_schema,
    filter_ops_undone_expired,
    ensure_job,
)


@graph
def gold_translation():
    """
    Translating an export of the JGI GOLD [1] SQL database to the NMDC database JSON schema.

    [1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
    """
    local_file_to_api_object(run_etl(build_merged_db()))


@graph()
def gold_translation_curation():
    # TODO
    #   - have produce_curated_db do actual curation (see notebook), persisting to db.
    #   - more steps in pipeline? Or handoff via run_status_sensor on PipelineRunStatus.SUCCESS.
    produce_curated_db(get_operation())


@graph()
def create_objects_from_site_object_puts():
    delete_operations(
        create_objects_from_ops(list_operations(filter_ops_done_object_puts()))
    )


@graph
def hello_pipeline():
    hello()


@graph
def hello_mongo():
    mongo_stats()


@graph
def update_terminus():
    """
    A pipeline definition. This example pipeline has a single solid.

    For more hints on writing Dagster pipelines, see our documentation overview on Pipelines:
    https://docs.dagster.io/overview/solids-pipelines/pipelines
    """
    update_schema()


@graph
def housekeeping():
    delete_operations(list_operations(filter_ops_undone_expired()))


@graph
def ensure_job_p():
    ensure_job()
