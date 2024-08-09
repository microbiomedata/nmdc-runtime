from dagster import graph

from nmdc_runtime.site.ops import (
    build_merged_db,
    nmdc_schema_database_export_filename,
    nmdc_schema_database_from_gold_study,
    nmdc_schema_object_to_dict,
    export_json_to_drs,
    get_gold_study_pipeline_inputs,
    gold_analysis_projects_by_study,
    gold_projects_by_study,
    gold_study,
    poll_for_run_completion,
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
    submit_metadata_to_db,
    filter_ops_undone_expired,
    construct_jobs,
    maybe_post_jobs,
    get_changesheet_in,
    perform_changesheet_updates,
    get_json_in,
    perform_mongo_updates,
    add_output_run_event,
    gold_biosamples_by_study,
    fetch_nmdc_portal_submission_by_id,
    translate_portal_submission_to_nmdc_schema_database,
    validate_metadata,
    neon_data_by_product,
    nmdc_schema_database_from_neon_soil_data,
    nmdc_schema_database_from_neon_benthic_data,
    nmdc_schema_database_from_neon_surface_water_data,
    nmdc_schema_database_export_filename_neon,
    get_neon_pipeline_mms_data_product,
    get_neon_pipeline_sls_data_product,
    get_neon_pipeline_surface_water_data_product,
    get_submission_portal_pipeline_inputs,
    get_csv_rows_from_url,
    get_neon_pipeline_benthic_data_product,
    get_neon_pipeline_inputs,
    get_df_from_url,
    site_code_mapping,
    materialize_alldocs,
    get_ncbi_export_pipeline_study,
    get_data_objects_from_biosamples,
    get_omics_processing_from_biosamples,
    get_library_preparation_from_biosamples,
    get_ncbi_export_pipeline_inputs,
    ncbi_submission_xml_from_nmdc_study,
    ncbi_submission_xml_asset,
)
from nmdc_runtime.site.export.study_metadata import get_biosamples_by_study_id


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
def housekeeping():
    delete_operations(list_operations(filter_ops_undone_expired()))


@graph
def ensure_alldocs():
    materialize_alldocs()


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
def gold_study_to_database():
    study_id = get_gold_study_pipeline_inputs()

    projects = gold_projects_by_study(study_id)
    biosamples = gold_biosamples_by_study(study_id)
    analysis_projects = gold_analysis_projects_by_study(study_id)
    study = gold_study(study_id)

    database = nmdc_schema_database_from_gold_study(
        study, projects, biosamples, analysis_projects
    )
    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename(study)

    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def translate_metadata_submission_to_nmdc_schema_database():
    (
        submission_id,
        omics_processing_mapping_file_url,
        data_object_mapping_file_url,
        biosample_extras_file_url,
        biosample_extras_slot_mapping_file_url,
    ) = get_submission_portal_pipeline_inputs()

    metadata_submission = fetch_nmdc_portal_submission_by_id(submission_id)
    omics_processing_mapping = get_csv_rows_from_url(omics_processing_mapping_file_url)
    data_object_mapping = get_csv_rows_from_url(data_object_mapping_file_url)
    biosample_extras = get_csv_rows_from_url(biosample_extras_file_url)
    biosample_extras_slot_mapping = get_csv_rows_from_url(
        biosample_extras_slot_mapping_file_url
    )

    database = translate_portal_submission_to_nmdc_schema_database(
        metadata_submission,
        omics_processing_mapping,
        data_object_mapping,
        biosample_extras=biosample_extras,
        biosample_extras_slot_mapping=biosample_extras_slot_mapping,
    )

    validate_metadata(database)

    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename(metadata_submission)
    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def ingest_metadata_submission():
    (
        submission_id,
        omics_processing_mapping_file_url,
        data_object_mapping_file_url,
        biosample_extras_file_url,
        biosample_extras_slot_mapping_file_url,
    ) = get_submission_portal_pipeline_inputs()

    metadata_submission = fetch_nmdc_portal_submission_by_id(submission_id)
    omics_processing_mapping = get_csv_rows_from_url(omics_processing_mapping_file_url)
    data_object_mapping = get_csv_rows_from_url(data_object_mapping_file_url)
    biosample_extras = get_csv_rows_from_url(biosample_extras_file_url)
    biosample_extras_slot_mapping = get_csv_rows_from_url(
        biosample_extras_slot_mapping_file_url
    )

    database = translate_portal_submission_to_nmdc_schema_database(
        metadata_submission,
        omics_processing_mapping,
        data_object_mapping,
        biosample_extras=biosample_extras,
        biosample_extras_slot_mapping=biosample_extras_slot_mapping,
    )
    run_id = submit_metadata_to_db(database)
    poll_for_run_completion(run_id)


@graph
def translate_neon_api_soil_metadata_to_nmdc_schema_database():
    mms_data_product = get_neon_pipeline_mms_data_product()
    sls_data_product = get_neon_pipeline_sls_data_product()

    mms_data = neon_data_by_product(mms_data_product)
    sls_data = neon_data_by_product(sls_data_product)

    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    database = nmdc_schema_database_from_neon_soil_data(
        mms_data, sls_data, neon_envo_mappings_file, neon_raw_data_file_mappings_file
    )

    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename_neon()

    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def ingest_neon_soil_metadata():
    mms_data_product = get_neon_pipeline_mms_data_product()
    sls_data_product = get_neon_pipeline_sls_data_product()

    mms_data = neon_data_by_product(mms_data_product)
    sls_data = neon_data_by_product(sls_data_product)

    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    database = nmdc_schema_database_from_neon_soil_data(
        mms_data, sls_data, neon_envo_mappings_file, neon_raw_data_file_mappings_file
    )
    run_id = submit_metadata_to_db(database)
    poll_for_run_completion(run_id)


@graph
def translate_neon_api_benthic_metadata_to_nmdc_schema_database():
    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
    ) = get_neon_pipeline_inputs()

    mms_benthic_data_product = get_neon_pipeline_benthic_data_product()
    mms_benthic = neon_data_by_product(mms_benthic_data_product)

    sites_mapping_dict = site_code_mapping()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    database = nmdc_schema_database_from_neon_benthic_data(
        mms_benthic,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
    )

    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename_neon()

    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def ingest_neon_benthic_metadata():
    mms_benthic_data_product = get_neon_pipeline_benthic_data_product()

    mms_benthic = neon_data_by_product(mms_benthic_data_product)

    sites_mapping_dict = site_code_mapping()

    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    database = nmdc_schema_database_from_neon_benthic_data(
        mms_benthic,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
    )
    run_id = submit_metadata_to_db(database)
    poll_for_run_completion(run_id)


@graph
def translate_neon_api_surface_water_metadata_to_nmdc_schema_database():
    mms_surface_water_data_product = get_neon_pipeline_surface_water_data_product()

    mms_surface_water = neon_data_by_product(mms_surface_water_data_product)

    sites_mapping_dict = site_code_mapping()

    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    database = nmdc_schema_database_from_neon_surface_water_data(
        mms_surface_water,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
    )

    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename_neon()

    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def ingest_neon_surface_water_metadata():
    mms_surface_water_data_product = get_neon_pipeline_surface_water_data_product()

    mms_surface_water = neon_data_by_product(mms_surface_water_data_product)

    sites_mapping_dict = site_code_mapping()

    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    database = nmdc_schema_database_from_neon_benthic_data(
        mms_surface_water,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
    )
    run_id = submit_metadata_to_db(database)
    poll_for_run_completion(run_id)


@graph
def nmdc_study_to_ncbi_submission_export():
    nmdc_study = get_ncbi_export_pipeline_study()
    ncbi_submission_metadata = get_ncbi_export_pipeline_inputs()
    biosamples = get_biosamples_by_study_id(nmdc_study)
    omics_processing_records = get_omics_processing_from_biosamples(biosamples)
    data_object_records = get_data_objects_from_biosamples(biosamples)
    library_preparation_records = get_library_preparation_from_biosamples(biosamples)
    xml_data = ncbi_submission_xml_from_nmdc_study(
        nmdc_study,
        ncbi_submission_metadata,
        biosamples,
        omics_processing_records,
        data_object_records,
        library_preparation_records,
    )
    ncbi_submission_xml_asset(xml_data)
