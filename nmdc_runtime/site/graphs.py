from dagster import graph

from nmdc_runtime.site.ops import (
    generate_biosample_set_for_nmdc_study_from_gold,
    nmdc_schema_database_export_filename,
    nmdc_schema_database_from_gold_study,
    nmdc_schema_object_to_dict,
    export_json_to_drs,
    get_gold_study_pipeline_inputs,
    gold_analysis_projects_by_study,
    gold_projects_by_study,
    gold_study,
    poll_for_run_completion,
    get_operation,
    produce_curated_db,
    delete_operations,
    create_objects_from_ops,
    list_operations,
    filter_ops_done_object_puts,
    hello,
    mongo_stats,
    run_script_to_update_insdc_biosample_identifiers,
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
    load_ontology,
    get_ncbi_export_pipeline_study,
    get_data_objects_from_biosamples,
    get_nucleotide_sequencing_from_biosamples,
    get_library_preparation_from_biosamples,
    get_all_instruments,
    get_ncbi_export_pipeline_inputs,
    ncbi_submission_xml_from_nmdc_study,
    ncbi_submission_xml_asset,
    render_text,
    get_database_updater_inputs,
    post_submission_portal_biosample_ingest_record_stitching_filename,
    generate_data_generation_set_post_biosample_ingest,
    get_instrument_ids_by_model,
    log_database_ids,
)
from nmdc_runtime.site.export.study_metadata import get_biosamples_by_study_id


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
def run_ontology_load():
    """
    A graph for loading ontologies.
    The source_ontology parameter is provided by the job configuration
    and passed to the load_ontology op.
    """
    load_ontology()


@graph
def ensure_jobs():
    jobs = construct_jobs()
    maybe_post_jobs(jobs)


@graph
def apply_changesheet():
    # Note: We use `_` as a "placeholder" variable.
    #       It's a variable to whose value we assign no significance. In this case, we use it to
    #       tell Dagster that one op depends upon the output of the other (so Dagster runs them
    #       in that order), without implying to maintainers that its value is significant to us.
    #       Reference (this strategy): https://docs.dagster.io/api/dagster/types#dagster.Nothing
    #       Reference (`_` variables): https://stackoverflow.com/a/47599668
    sheet_in = get_changesheet_in()
    outputs = perform_changesheet_updates(sheet_in)
    _ = add_output_run_event(outputs)
    materialize_alldocs(waits_for=_)


@graph
def apply_metadata_in():
    # Note: We use `_` as a "placeholder" variable.
    outputs = perform_mongo_updates(get_json_in())
    _ = add_output_run_event(outputs)
    materialize_alldocs(waits_for=_)


@graph
def gold_study_to_database():
    (
        study_id,
        study_type,
        gold_nmdc_instrument_mapping_file_url,
        include_field_site_info,
        enable_biosample_filtering,
    ) = get_gold_study_pipeline_inputs()

    projects = gold_projects_by_study(study_id)
    biosamples = gold_biosamples_by_study(study_id)
    analysis_projects = gold_analysis_projects_by_study(study_id)
    study = gold_study(study_id)
    gold_nmdc_instrument_map_df = get_df_from_url(gold_nmdc_instrument_mapping_file_url)

    database = nmdc_schema_database_from_gold_study(
        study,
        study_type,
        projects,
        biosamples,
        analysis_projects,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename(study)

    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def translate_metadata_submission_to_nmdc_schema_database():
    (
        submission_id,
        nucleotide_sequencing_mapping_file_url,
        data_object_mapping_file_url,
        biosample_extras_file_url,
        biosample_extras_slot_mapping_file_url,
    ) = get_submission_portal_pipeline_inputs()

    metadata_submission = fetch_nmdc_portal_submission_by_id(submission_id)
    nucleotide_sequencing_mapping = get_csv_rows_from_url(
        nucleotide_sequencing_mapping_file_url
    )
    data_object_mapping = get_csv_rows_from_url(data_object_mapping_file_url)
    biosample_extras = get_csv_rows_from_url(biosample_extras_file_url)
    biosample_extras_slot_mapping = get_csv_rows_from_url(
        biosample_extras_slot_mapping_file_url
    )
    instrument_mapping = get_instrument_ids_by_model()

    database = translate_portal_submission_to_nmdc_schema_database(
        metadata_submission,
        nucleotide_sequencing_mapping=nucleotide_sequencing_mapping,
        data_object_mapping=data_object_mapping,
        biosample_extras=biosample_extras,
        biosample_extras_slot_mapping=biosample_extras_slot_mapping,
        instrument_mapping=instrument_mapping,
    )

    validate_metadata(database)

    log_database_ids(database)

    database_dict = nmdc_schema_object_to_dict(database)
    filename = nmdc_schema_database_export_filename(metadata_submission)
    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def ingest_metadata_submission():
    (
        submission_id,
        nucleotide_sequencing_mapping_file_url,
        data_object_mapping_file_url,
        biosample_extras_file_url,
        biosample_extras_slot_mapping_file_url,
    ) = get_submission_portal_pipeline_inputs()

    metadata_submission = fetch_nmdc_portal_submission_by_id(submission_id)
    nucleotide_sequencing_mapping = get_csv_rows_from_url(
        nucleotide_sequencing_mapping_file_url
    )
    data_object_mapping = get_csv_rows_from_url(data_object_mapping_file_url)
    biosample_extras = get_csv_rows_from_url(biosample_extras_file_url)
    biosample_extras_slot_mapping = get_csv_rows_from_url(
        biosample_extras_slot_mapping_file_url
    )
    instrument_mapping = get_instrument_ids_by_model()

    database = translate_portal_submission_to_nmdc_schema_database(
        metadata_submission,
        nucleotide_sequencing_mapping=nucleotide_sequencing_mapping,
        data_object_mapping=data_object_mapping,
        biosample_extras=biosample_extras,
        biosample_extras_slot_mapping=biosample_extras_slot_mapping,
        instrument_mapping=instrument_mapping,
    )

    log_database_ids(database)

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
        neon_nmdc_instrument_mapping_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    neon_nmdc_instrument_mapping_file = get_df_from_url(
        neon_nmdc_instrument_mapping_file_url
    )

    database = nmdc_schema_database_from_neon_soil_data(
        mms_data,
        sls_data,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
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
        neon_nmdc_instrument_mapping_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    neon_nmdc_instrument_mapping_file = get_df_from_url(
        neon_nmdc_instrument_mapping_file_url
    )

    database = nmdc_schema_database_from_neon_soil_data(
        mms_data,
        sls_data,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
    )
    run_id = submit_metadata_to_db(database)
    poll_for_run_completion(run_id)


@graph
def translate_neon_api_benthic_metadata_to_nmdc_schema_database():
    (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
        neon_nmdc_instrument_mapping_file_url,
    ) = get_neon_pipeline_inputs()

    mms_benthic_data_product = get_neon_pipeline_benthic_data_product()
    mms_benthic = neon_data_by_product(mms_benthic_data_product)

    sites_mapping_dict = site_code_mapping()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    neon_nmdc_instrument_mapping_file = get_df_from_url(
        neon_nmdc_instrument_mapping_file_url
    )

    database = nmdc_schema_database_from_neon_benthic_data(
        mms_benthic,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
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
        neon_nmdc_instrument_mapping_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    neon_nmdc_instrument_mapping_file = get_df_from_url(
        neon_nmdc_instrument_mapping_file_url
    )

    database = nmdc_schema_database_from_neon_benthic_data(
        mms_benthic,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
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
        neon_nmdc_instrument_mapping_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    neon_nmdc_instrument_mapping_file = get_df_from_url(
        neon_nmdc_instrument_mapping_file_url
    )

    database = nmdc_schema_database_from_neon_surface_water_data(
        mms_surface_water,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
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
        neon_nmdc_instrument_mapping_file_url,
    ) = get_neon_pipeline_inputs()

    neon_envo_mappings_file = get_df_from_url(neon_envo_mappings_file_url)

    neon_raw_data_file_mappings_file = get_df_from_url(
        neon_raw_data_file_mappings_file_url
    )

    neon_nmdc_instrument_mapping_file = get_df_from_url(
        neon_nmdc_instrument_mapping_file_url
    )

    database = nmdc_schema_database_from_neon_benthic_data(
        mms_surface_water,
        sites_mapping_dict,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
    )
    run_id = submit_metadata_to_db(database)
    poll_for_run_completion(run_id)


@graph
def nmdc_study_to_ncbi_submission_export():
    nmdc_study = get_ncbi_export_pipeline_study()
    ncbi_submission_metadata = get_ncbi_export_pipeline_inputs()
    biosamples = get_biosamples_by_study_id(nmdc_study)
    nucleotide_sequencing_records = get_nucleotide_sequencing_from_biosamples(
        biosamples
    )
    data_object_records = get_data_objects_from_biosamples(biosamples)
    library_preparation_records = get_library_preparation_from_biosamples(biosamples)
    all_instruments = get_all_instruments()
    xml_data = ncbi_submission_xml_from_nmdc_study(
        nmdc_study,
        ncbi_submission_metadata,
        biosamples,
        nucleotide_sequencing_records,
        data_object_records,
        library_preparation_records,
        all_instruments,
    )
    ncbi_submission_xml_asset(xml_data)


@graph
def generate_data_generation_set_for_biosamples_in_nmdc_study():
    (
        study_id,
        gold_nmdc_instrument_mapping_file_url,
        include_field_site_info,
        enable_biosample_filtering,
    ) = get_database_updater_inputs()
    gold_nmdc_instrument_map_df = get_df_from_url(gold_nmdc_instrument_mapping_file_url)

    database = generate_data_generation_set_post_biosample_ingest(
        study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )

    database_dict = nmdc_schema_object_to_dict(database)
    filename = post_submission_portal_biosample_ingest_record_stitching_filename(
        study_id
    )
    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def generate_biosample_set_from_samples_in_gold():
    (
        study_id,
        gold_nmdc_instrument_mapping_file_url,
        include_field_site_info,
        enable_biosample_filtering,
    ) = get_database_updater_inputs()
    gold_nmdc_instrument_map_df = get_df_from_url(gold_nmdc_instrument_mapping_file_url)

    database = generate_biosample_set_for_nmdc_study_from_gold(
        study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    database_dict = nmdc_schema_object_to_dict(database)
    filename = post_submission_portal_biosample_ingest_record_stitching_filename(
        study_id
    )
    outputs = export_json_to_drs(database_dict, filename)
    add_output_run_event(outputs)


@graph
def generate_update_script_for_insdc_biosample_identifiers():
    """Generate a MongoDB update script to add INSDC biosample identifiers to biosamples based on GOLD data.

    This graph fetches the necessary inputs, then calls the run_script_to_update_insdc_biosample_identifiers op
    to generate a script for updating biosample records with INSDC identifiers obtained from GOLD.
    The script is returned as a dictionary that can be executed against MongoDB.
    """
    (
        study_id,
        gold_nmdc_instrument_mapping_file_url,
        include_field_site_info,
        enable_biosample_filtering,
    ) = get_database_updater_inputs()
    gold_nmdc_instrument_map_df = get_df_from_url(gold_nmdc_instrument_mapping_file_url)

    update_script = run_script_to_update_insdc_biosample_identifiers(
        study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    render_text(update_script)
