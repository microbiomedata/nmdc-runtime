"""
Get NMDC study-associated metadata from search api
"""

import csv
from io import StringIO

from dagster import (
    op,
    get_dagster_logger,
    graph,
    OpExecutionContext,
    AssetMaterialization,
)
from toolz import get_in

from nmdc_runtime.api.core.util import hash_from_str
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.site.ops import add_output_run_event
from nmdc_runtime.site.resources import RuntimeApiSiteClient
from nmdc_runtime.util import flatten


def get_all_docs(client, collection, filter_):
    per_page = 200
    url_base = f"/{collection}?filter={filter_}&per_page={per_page}"
    results = []
    response = client.request("GET", url_base)
    if response.status_code != 200:
        raise Exception(
            f"Runtime API request failed with status {response.status_code}."
            f" Check URL: {url_base}"
        )
    rv = response.json()
    results.extend(rv.get("results", []))
    page, count = rv["meta"]["page"], rv["meta"]["count"]
    assert count <= 10_000
    while page * per_page < count:
        page += 1
        url = f"{url_base}&page={page}"
        response = client.request("GET", url)
        if response.status_code != 200:
            raise Exception(
                f"Runtime API request failed with status {response.status_code}."
                f" Check URL: {url}"
            )
        rv = response.json()
        results.extend(rv.get("results", []))
    return results


@op(required_resource_keys={"runtime_api_site_client"})
def get_study_biosamples_metadata(context):
    study_id = context.op_config.get("study_id")
    username = context.op_config.get("username")
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    # study id -> biosample ids (via part_of: Study)
    biosamples = get_all_docs(client, "biosamples", f"part_of:{study_id}")

    # biosample id -> omics processing ids (via has_input: Biosample)
    # omics processing id -> omics types (as set(omics_type.has_raw_value))

    logger = get_dagster_logger()

    for i, biosample in enumerate(biosamples):
        b_id = biosample["id"]
        logger.info(
            f"({i+1}/{len(biosamples)}): getting omics processing docs for biosample {b_id}..."
        )
        omics_processing_docs = get_all_docs(
            client, "activities", f"type:nmdc:OmicsProcessing,has_input:{b_id}"
        )
        omics_types = {
            get_in(["omics_type", "has_raw_value"], d) for d in omics_processing_docs
        }
        biosample["omics_types"] = sorted(filter(None, omics_types))

    # flatten biosample json to (sorted) dotted-path field names for csv headers.
    context.log.info(biosamples)
    flattened_docs = [flatten(b) for b in biosamples]

    # export csv
    return dict(study_id=study_id, flattened_docs=flattened_docs, username=username)


@op(required_resource_keys={"mongo"})
def export_study_biosamples_as_csv(context: OpExecutionContext, study_export_info):
    # not all docs have every field
    fieldnames = set()
    for doc in study_export_info["flattened_docs"]:
        fieldnames |= set(doc.keys())
    fieldnames = sorted(fieldnames)

    f = StringIO()
    filename = f'study_{study_export_info["study_id"].replace(":", "_")}_biosamples.csv'
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for doc in study_export_info["flattened_docs"]:
        writer.writerow(doc)

    # If exists, get existing drs object id.
    mdb = context.resources.mongo.db
    content = f.getvalue()
    sha256hash = hash_from_str(content, "sha256")
    drs_object = mdb.objects.find_one(
        {"checksums": {"$elemMatch": {"type": "sha256", "checksum": sha256hash}}}
    )
    if drs_object is None:
        drs_object = persist_content_and_get_drs_object(
            content=f.getvalue(),
            username=study_export_info["username"],
            filename=filename,
            content_type="text/csv",
            description=f"{study_export_info['study_id']} biosamples",
            id_ns="study-metadata-export-csv",
        )
    context.log_event(
        AssetMaterialization(
            asset_key=filename,
            description=f"{study_export_info['study_id']} biosamples",
        )
    )
    return ["/objects/" + drs_object["id"]]


@graph
def export_study_biosamples_metadata():
    outputs = export_study_biosamples_as_csv(get_study_biosamples_metadata())
    add_output_run_event(outputs)


@op(required_resource_keys={"runtime_api_site_client"})
def get_biosamples_by_study_id(context: OpExecutionContext, nmdc_study: dict):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    biosamples = get_all_docs(client, "biosamples", f"part_of:{nmdc_study['id']}")
    return biosamples
