import json

from dagster import (
    repository,
    ScheduleDefinition,
    asset_sensor,
    AssetKey,
    SkipReason,
    RunRequest,
    sensor,
)
from starlette import status
from toolz import merge

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.site.graphs import (
    gold_translation,
    gold_translation_curation,
    create_objects_from_site_object_puts,
    housekeeping,
    ensure_job,
)
from nmdc_runtime.site.resources import mongo_resource, get_mongo
from nmdc_runtime.site.resources import (
    runtime_api_site_client_resource,
    get_runtime_api_site_client,
)
from nmdc_runtime.site.resources import terminus_resource
from nmdc_runtime.site.translation.jgi import jgi_job, test_jgi_job
from nmdc_runtime.util import frozendict_recursive

preset_normal = {
    "config": {
        "resources": {
            "runtime_api_site_client": {
                "config": {
                    "base_url": {"env": "API_HOST"},
                    "site_id": {"env": "API_SITE_ID"},
                    "client_id": {"env": "API_SITE_CLIENT_ID"},
                    "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                },
            },
            "terminus": {
                "config": {
                    "server_url": {"env": "TERMINUS_SERVER_URL"},
                    "key": {"env": "TERMINUS_KEY"},
                    "user": {"env": "TERMINUS_USER"},
                    "account": {"env": "TERMINUS_ACCOUNT"},
                    "dbid": {"env": "TERMINUS_DBID"},
                },
            },
            "mongo": {
                "config": {
                    "host": {"env": "MONGO_HOST"},
                    "username": {"env": "MONGO_USERNAME"},
                    "password": {"env": "MONGO_PASSWORD"},
                    "dbname": {"env": "MONGO_DBNAME"},
                },
            },
        },
    },
    "resource_defs": {
        "runtime_api_site_client": runtime_api_site_client_resource,
        "terminus": terminus_resource,
        "mongo": mongo_resource,
    },
}

run_config_frozen__normal_env = frozendict_recursive(preset_normal["config"])

housekeeping_weekly = ScheduleDefinition(
    name="housekeeping_weekly",
    cron_schedule="45 6 * * 1",
    execution_timezone="America/New_York",
    job=housekeeping.to_job(**preset_normal),
)


def asset_materialization_metadata(asset_event, key):
    """Get metadata from an asset materialization event.

    Example:
    > object_id_latest = asset_materialization_metadata(asset_event, "object_id_latest").text
    """
    for (
        e
    ) in (
        asset_event.dagster_event.step_materialization_data.materialization.metadata_entries
    ):
        if e.label == key:
            return e.entry_data
    return None


@sensor(job=ensure_job.to_job(**preset_normal))
def metaproteomics_analysis_activity_ingest(_context):
    wf_id = "metap-metadata-1.0.0"
    mdb = get_mongo(run_config=run_config_frozen__normal_env).db
    latest = mdb.objects.find_one(
        {"types": "metaproteomics_analysis_activity_set"}, sort=[("created_time", -1)]
    )
    object_id_latest = latest["id"]
    existing_job = mdb.jobs.find_one(
        {"workflow.id": wf_id, "config.object_id_latest": object_id_latest},
    )
    if not existing_job:
        run_config = merge(
            run_config_frozen__normal_env,
            {
                "solids": {
                    "construct_job": {
                        "config": {
                            "job_base": {"workflow": {"id": wf_id}},
                            "object_id_latest": object_id_latest,
                        }
                    }
                }
            },
        )
        yield RunRequest(run_key=object_id_latest, run_config=run_config)
    else:
        yield SkipReason(f"Already ensured job for {object_id_latest} for {wf_id}")


@asset_sensor(
    asset_key=AssetKey(["object", "nmdc_database.json.zip"]),
    job=ensure_job.to_job(**preset_normal),
)
def ensure_gold_translation_job(_context, asset_event):
    mdb = get_mongo(run_config=run_config_frozen__normal_env).db
    gold_etl_latest = mdb.objects.find_one(
        {"name": "nmdc_database.json.zip"}, sort=[("created_time", -1)]
    )
    sensed_object_id = asset_materialization_metadata(asset_event, "object_id").text
    if gold_etl_latest is None:
        yield SkipReason("can't find sensed asset object_id in database")
        return
    elif gold_etl_latest["id"] != sensed_object_id:
        yield SkipReason("later object than sensed materialization")
        return

    run_config = merge(
        run_config_frozen__normal_env,
        {
            "solids": {
                "construct_job": {
                    "config": {
                        "job_base": {"workflow": {"id": "gold-translation-1.0.0"}},
                        "object_id_latest": gold_etl_latest["id"],
                    }
                }
            }
        },
    )
    yield RunRequest(run_key=sensed_object_id, run_config=run_config)


@asset_sensor(
    asset_key=AssetKey(["job", "gold-translation-1.0.0"]),
    job=gold_translation_curation.to_job(**preset_normal),
)
def claim_and_run_gold_translation_curation(_context, asset_event):
    client = get_runtime_api_site_client(run_config=run_config_frozen__normal_env)
    mdb = get_mongo(run_config=run_config_frozen__normal_env).db
    object_id_latest = asset_materialization_metadata(
        asset_event, "object_id_latest"
    ).text
    job = mdb.jobs.find_one(
        {
            "workflow.id": "gold-translation-1.0.0",
            "config.object_id_latest": object_id_latest,
        }
    )
    if job is not None:
        rv = client.claim_job(job["id"])
        if rv.status_code == status.HTTP_200_OK:
            operation = rv.json()
            run_config = merge(
                run_config_frozen__normal_env,
                {
                    "ops": {
                        "get_operation": {
                            "config": {
                                "operation_id": operation["id"],
                            }
                        }
                    }
                },
            )
            yield RunRequest(run_key=operation["id"], run_config=run_config)
        else:
            yield SkipReason("Job found, but already claimed by this site")
    else:
        yield SkipReason("No job found")


@sensor(job=create_objects_from_site_object_puts.to_job(**preset_normal))
def done_object_put_ops(_context):
    client = get_runtime_api_site_client(run_config=run_config_frozen__normal_env)
    ops = [
        op.dict()
        for op in client.list_operations(
            {
                "filter": json.dumps(
                    {
                        "done": True,
                        "metadata.model": dotted_path_for(ObjectPutMetadata),
                    }
                )
            }
        )
    ]
    should_run = len(ops) > 0
    if should_run:
        run_key = ",".join(sorted([op["id"] for op in ops]))
        yield RunRequest(run_key=run_key, run_config=run_config_frozen__normal_env)


@repository
def repo():
    graph_jobs = [
        gold_translation.to_job(**preset_normal),
    ]
    schedules = [housekeeping_weekly]
    sensors = [
        done_object_put_ops,
        ensure_gold_translation_job,
        claim_and_run_gold_translation_curation,
        metaproteomics_analysis_activity_ingest,
    ]

    return graph_jobs + schedules + sensors


@repository
def translation():
    graph_jobs = [jgi_job]

    return graph_jobs


@repository
def test_translation():
    graph_jobs = [test_jgi_job]

    return graph_jobs
