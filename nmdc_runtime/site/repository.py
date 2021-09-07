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
from toolz import merge, get_in

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.api.models.trigger import Trigger
from nmdc_runtime.site.graphs import (
    gold_translation,
    gold_translation_curation,
    create_objects_from_site_object_puts,
    housekeeping,
    ensure_jobs,
)
from nmdc_runtime.site.resources import mongo_resource, get_mongo
from nmdc_runtime.site.resources import (
    runtime_api_site_client_resource,
    get_runtime_api_site_client,
)
from nmdc_runtime.site.resources import terminus_resource
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
    > object_id = asset_materialization_metadata(asset_event, "object_id").text
    """
    for (
        e
    ) in (
        asset_event.dagster_event.step_materialization_data.materialization.metadata_entries
    ):
        if e.label == key:
            return e.entry_data
    return None


@sensor(job=ensure_jobs.to_job(**preset_normal))
def process_workflow_job_triggers(_context):
    """Post a workflow job for each new object with an object_type matching an active trigger.
    (Source: nmdc_runtime.api.boot.triggers).
    """
    mdb = get_mongo(run_config=run_config_frozen__normal_env).db
    triggers = [Trigger(**d) for d in mdb.triggers.find()]
    base_jobs = []
    for t in triggers:
        wf_id, object_type = t.workflow_id, t.object_type_id
        object_ids_of_type = [
            d["id"] for d in mdb.objects.find({"types": object_type}, ["id"])
        ]

        if len(object_ids_of_type) == 0:
            continue

        object_ids_with_existing_jobs = [
            get_in(["config", "object_id"], d)
            for d in mdb.jobs.find(
                {"workflow.id": wf_id, "config.object_id": {"$in": object_ids_of_type}},
                ["config.object_id"],
            )
        ]
        object_ids_needing_jobs = list(
            set(object_ids_of_type) - set(object_ids_with_existing_jobs)
        )
        base_jobs.extend(
            [
                {
                    "workflow": {"id": wf_id},
                    "config": {"object_id": id_},
                }
                for id_ in object_ids_needing_jobs
            ]
        )

    if len(base_jobs) > 0:
        run_config = merge(
            run_config_frozen__normal_env,
            {"ops": {"construct_jobs": {"config": {"base_jobs": base_jobs}}}},
        )
        run_key = str(
            tuple(
                sorted(
                    (get_in(["workflow", "id"], b), get_in(["config", "object_id"], b))
                    for b in base_jobs
                )
            )
        )
        yield RunRequest(run_key=run_key, run_config=run_config)
    else:
        yield SkipReason(f"No new jobs required")


@asset_sensor(
    asset_key=AssetKey(["object", "nmdc_database.json.zip"]),
    job=ensure_jobs.to_job(**preset_normal),
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
                "construct_jobs": {
                    "config": {
                        "base_jobs": [
                            {
                                "workflow": {"id": "gold-translation-1.0.0"},
                                "config": {"object_id": gold_etl_latest["id"]},
                            }
                        ]
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
        process_workflow_job_triggers,
    ]

    return graph_jobs + schedules + sensors


# @repository
# def translation():
#     graph_jobs = [jgi_job, gold_job, emsl_job]
#
#     return graph_jobs
#
#
# @repository
# def test_translation():
#     graph_jobs = [test_jgi_job, test_gold_job, test_emsl_job]
#
#     return graph_jobs
#
#
# @repository
# def validation():
#     graph_jobs = [validate_jgi_job, validate_gold_job, validate_emsl_job]
#     return graph_jobs
#
#
# @repository
# def test_validation():
#     graph_jobs = [test_validate_jgi_job, test_validate_gold_job, test_validate_emsl_job]
#     return graph_jobs
