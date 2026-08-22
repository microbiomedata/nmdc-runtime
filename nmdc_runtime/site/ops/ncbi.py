"""
Dagster ops related to NCBI.

Note: These were extracted from a 1900-line file at `nmdc_runtime/site/ops.py` during a refactor.
"""

import os

from dagster import (
    Any,
    AssetMaterialization,
    Dict,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
    Field,
    Permissive,
)

from nmdc_runtime.api.endpoints.find import find_study_by_id
from nmdc_runtime.site.export.ncbi_xml import NCBISubmissionXML
from nmdc_runtime.site.export.ncbi_xml_utils import (
    fetch_data_objects_from_biosamples,
    fetch_nucleotide_sequencing_from_biosamples,
    fetch_library_preparation_from_biosamples,
)


@op(
    description="NCBI Submission XML file rendered in a Dagster Asset",
    out=Out(description="XML content rendered through Dagit UI"),
)
def ncbi_submission_xml_asset(context: OpExecutionContext, data: str):
    filename = "ncbi_submission.xml"
    file_path = os.path.join(context.instance.storage_directory(), filename)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as f:
        f.write(data)

    context.log_event(
        AssetMaterialization(
            asset_key="ncbi_submission_xml",
            description="NCBI Submission XML Data",
            metadata={
                "file_path": MetadataValue.path(file_path),
                "xml": MetadataValue.text(data),
            },
        )
    )

    return Output(data)


@op(config_schema={"nmdc_study_id": str}, required_resource_keys={"mongo"})
def get_ncbi_export_pipeline_study(context: OpExecutionContext) -> Any:
    nmdc_study = find_study_by_id(
        context.op_config["nmdc_study_id"], context.resources.mongo.db
    )
    return nmdc_study


@op(
    config_schema={
        "nmdc_ncbi_attribute_mapping_file_url": str,
        "ncbi_submission_metadata": Field(
            Permissive(
                {
                    "organization": String,
                }
            ),
            is_required=True,
            description="General metadata about the NCBI submission.",
        ),
        "ncbi_biosample_metadata": Field(
            Permissive(
                {
                    "organism_name": String,
                }
            ),
            is_required=True,
            description="Metadata for one or many NCBI BioSample in the Submission.",
        ),
    },
    out=Out(Dict),
)
def get_ncbi_export_pipeline_inputs(context: OpExecutionContext) -> str:
    nmdc_ncbi_attribute_mapping_file_url = context.op_config[
        "nmdc_ncbi_attribute_mapping_file_url"
    ]
    ncbi_submission_metadata = context.op_config.get("ncbi_submission_metadata", {})
    ncbi_biosample_metadata = context.op_config.get("ncbi_biosample_metadata", {})

    return {
        "nmdc_ncbi_attribute_mapping_file_url": nmdc_ncbi_attribute_mapping_file_url,
        "ncbi_submission_metadata": ncbi_submission_metadata,
        "ncbi_biosample_metadata": ncbi_biosample_metadata,
    }


@op(required_resource_keys={"mongo"})
def get_aggregated_pooled_biosamples(context: OpExecutionContext, biosamples: list):
    from nmdc_runtime.site.export.ncbi_xml_utils import check_pooling_for_biosamples

    mdb = context.resources.mongo.db
    material_processing_set = mdb["material_processing_set"]
    pooled_biosamples_data = check_pooling_for_biosamples(
        material_processing_set, biosamples
    )

    # Fetch ProcessedSample names from database
    processed_sample_ids = set()
    for biosample_id, pooling_info in pooled_biosamples_data.items():
        if pooling_info and pooling_info.get("processed_sample_id"):
            processed_sample_ids.add(pooling_info["processed_sample_id"])

    # Query database for ProcessedSample names
    if processed_sample_ids:
        processed_sample_set = mdb["processed_sample_set"]
        cursor = processed_sample_set.find(
            {"id": {"$in": list(processed_sample_ids)}}, {"id": 1, "name": 1}
        )
        processed_samples = {doc["id"]: doc.get("name", "") for doc in cursor}

        # Update pooled_biosamples_data with ProcessedSample names
        for biosample_id, pooling_info in pooled_biosamples_data.items():
            if pooling_info and pooling_info.get("processed_sample_id"):
                processed_sample_id = pooling_info["processed_sample_id"]
                if processed_sample_id in processed_samples:
                    pooling_info["processed_sample_name"] = processed_samples[
                        processed_sample_id
                    ]

    return pooled_biosamples_data


@op(required_resource_keys={"mongo"})
def get_data_objects_from_biosamples(context: OpExecutionContext, biosamples: list):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    data_object_set = mdb["data_object_set"]
    biosample_data_objects = fetch_data_objects_from_biosamples(
        alldocs_collection, data_object_set, biosamples
    )
    return biosample_data_objects


@op(required_resource_keys={"mongo"})
def get_nucleotide_sequencing_from_biosamples(
    context: OpExecutionContext, biosamples: list
):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    data_generation_set = mdb["data_generation_set"]
    biosample_omics_processing = fetch_nucleotide_sequencing_from_biosamples(
        alldocs_collection, data_generation_set, biosamples
    )
    return biosample_omics_processing


@op(required_resource_keys={"mongo"})
def get_library_preparation_from_biosamples(
    context: OpExecutionContext, biosamples: list
):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    material_processing_set = mdb["material_processing_set"]
    biosample_lib_prep = fetch_library_preparation_from_biosamples(
        alldocs_collection, material_processing_set, biosamples
    )
    return biosample_lib_prep


@op
def ncbi_submission_xml_from_nmdc_study(
    context: OpExecutionContext,
    nmdc_study: Any,
    ncbi_exporter_metadata: dict,
    biosamples: list,
    omics_processing_records: list,
    data_object_records: list,
    library_preparation_records: list,
    all_instruments: dict,
    pooled_biosamples_data: dict,
) -> str:
    ncbi_exporter = NCBISubmissionXML(nmdc_study, ncbi_exporter_metadata)
    ncbi_xml = ncbi_exporter.get_submission_xml(
        biosamples,
        omics_processing_records,
        data_object_records,
        library_preparation_records,
        all_instruments,
        pooled_biosamples_data,
    )
    return ncbi_xml
