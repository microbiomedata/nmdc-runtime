import pymongo.database
from dagster import solid, AssetMaterialization, AssetKey, EventMetadata, Output
from toolz import get_in

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.models.job import JobBase, Job


@solid(required_resource_keys={"mongo"})
def ensure_job(context):
    mdb: pymongo.database.Database = context.resources.mongo.db
    job = JobBase(**context.solid_config["job_base"])
    object_id_latest = context.solid_config["object_id_latest"]

    job_docs = list(mdb.jobs.find({"workflow.id": job.workflow.id}))
    job_docs_older = [
        d
        for d in job_docs
        if get_in(["config", "object_id_latest"], d) != object_id_latest
    ]
    if len(job_docs) == len(job_docs_older):
        doc = {
            "id": generate_id_unique(mdb, "jobs"),
            "workflow": {"id": job.workflow.id},
            "config": {"object_id_latest": object_id_latest},
        }
        job = Job(**doc)
        mdb.jobs.insert_one(doc)
        yield AssetMaterialization(
            asset_key=AssetKey(["job", job.workflow.id]),
            description=f"workflow job",
            metadata={
                "object_id_latest": EventMetadata.text(object_id_latest),
            },
        )
    if len(job_docs_older):
        mdb.jobs.delete_many({"id": {"$in": [d["id"] for d in job_docs_older]}})

    job = Job(**mdb.jobs.find_one({"workflow.id": job.workflow.id}))
    yield Output(job.dict(exclude_unset=True))
