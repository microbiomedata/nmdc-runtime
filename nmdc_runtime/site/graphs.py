from dagster import graph

from nmdc_runtime.site.ops import (
    build_merged_db,
    database_from_gold_study,
    export_json,
    get_gold_study_pipeline_inputs,
    gold_analysis_projects_by_study,
    gold_projects_by_study,
    gold_study,
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
    construct_jobs,
    maybe_post_jobs,
    get_changesheet_in,
    perform_changesheet_updates,
    get_json_in,
    perform_mongo_updates,
    add_output_run_event,
    gold_biosamples_by_study,
    gold_biosample_ids,
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
    #   - more steps in pipeline? Or handoff via run_status_sensor on DagsterRunStatus.SUCCESS.
    produce_curated_db(get_operation())


@graph()
def create_objects_from_site_object_puts():
    delete_operations(
        create_objects_from_ops(list_operations(filter_ops_done_object_puts()))
    )


@graph
def hello_graph():
    return hello()


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
def ensure_jobs():
    jobs = construct_jobs()
    maybe_post_jobs(jobs)


@graph
def apply_changesheet():
    sheet_in = get_changesheet_in()
    outputs = perform_changesheet_updates(sheet_in)
    add_output_run_event(outputs)


@graph
def apply_metadata_in():
    outputs = perform_mongo_updates(get_json_in())
    add_output_run_event(outputs)


@graph
def get_gold_biosample_ids():
    biosamples = gold_biosamples_by_study()
    output_config = gold_biosample_ids(biosamples)
    outputs = export_json(output_config)
    add_output_run_event(outputs)


@graph
def gold_study_to_database():
    inputs = get_gold_study_pipeline_inputs()

    projects = gold_projects_by_study(inputs)
    biosamples = gold_biosamples_by_study(inputs)
    analysis_projects = gold_analysis_projects_by_study(inputs)
    study = gold_study(inputs)

    database = database_from_gold_study(study, projects, biosamples, analysis_projects)

    outputs = export_json(database)
    add_output_run_event(outputs)
