import json
from datetime import datetime, timezone

from toolz import dissoc

from nmdc_runtime.api.models.job import JobOperationMetadata
from nmdc_runtime.api.models.operation import Operation
from nmdc_runtime.api.models.operation import UpdateOperationRequest
from nmdc_runtime.api.models.util import ListRequest
from nmdc_runtime.api.models.util import ResultT


def load_local_json(url, prefixes_url_to_local=None):
    """Useful for large files cached on local filesystem.

    You may, for example, `cp --parents ` many files on a remote filesystem to a staging
    folder on that remote filesystem, gzip that folder, scp it to your local machine, and then
    extract to your local machine.

    Example:
    prefixes_url_to_local = {
        "https://data.microbiomedata.org/data/": "/Users/dwinston/nmdc_files/2021-09-scanon-meta/ficus/pipeline_products/",
        "https://portal.nersc.gov/project/m3408/": "/Users/dwinston/nmdc_files/2021-09-scanon-meta/www/",
    }
    """
    path = url
    for before, after in prefixes_url_to_local.items():
        path = path.replace(before, after)
    with open(path) as f:
        return json.load(f)


def claim_metadata_ingest_jobs(
    client, drs_object_ids_to_ingest, wf_id, max_page_size=1000
):
    lr = ListRequest(
        filter=json.dumps(
            {
                "workflow.id": wf_id,
                "config.object_id": {"$in": drs_object_ids_to_ingest},
            }
        ),
        max_page_size=max_page_size,
    )
    jobs = []
    while True:
        rv = client.list_jobs(lr.model_dump()).json()
        jobs.extend(rv["resources"])
        if "next_page_token" not in rv:
            break
        else:
            lr.page_token = rv["next_page_token"]

        # safety escape
        if len(jobs) == len(drs_object_ids_to_ingest):
            break

    job_claim_responses = [client.claim_job(j["id"]) for j in jobs]

    return job_claim_responses


def mongo_add_docs_result_as_dict(rv):
    return {
        collection_name: dissoc(bulk_write_result.bulk_api_result, "upserted")
        for collection_name, bulk_write_result in rv.items()
    }


def get_metadata_ingest_job_ops(mongo, wf_id, drs_object_ids_to_ingest):
    return list(
        mongo.db.operations.find(
            {
                "metadata.job.workflow.id": wf_id,
                "metadata.job.config.object_id": {"$in": drs_object_ids_to_ingest},
                "done": False,
            }
        )
    )


def do_metadata_ingest_job(client, mongo, job_op_doc):
    op = Operation[ResultT, JobOperationMetadata](**job_op_doc)
    object_info = client.get_object_info(op.metadata.job.config["object_id"]).json()
    url = object_info["access_methods"][0]["access_url"]["url"]
    docs = load_local_json(url)
    op_result = mongo.add_docs(docs, validate=False, replace=False)
    op_patch = UpdateOperationRequest(
        done=True,
        result=mongo_add_docs_result_as_dict(op_result),
        metadata={"done_at": datetime.now(timezone.utc).isoformat(timespec="seconds")},
    )
    return client.update_operation(op.id, op_patch)
