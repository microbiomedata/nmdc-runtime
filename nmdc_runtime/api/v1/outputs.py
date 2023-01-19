from fastapi import APIRouter, Depends, HTTPException
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.site import Site, get_current_client_site
from pymongo import ReturnDocument
from pymongo.database import Database as MongoDatabase
from pymongo.errors import DuplicateKeyError
from starlette import status

router = APIRouter(prefix="/outputs", tags=["outputs"])


# @router.post(
#     "",
#     status_code=status.HTTP_201_CREATED,
# )
# async def ingest(
#     # ingest: Ingest,
#     mdb: MongoDatabase = Depends(get_mongo_db),
#     # site: Site = Depends(get_current_client_site),
# ) -> bool:
#     pass
#     # try:

<<<<<<< HEAD
        if site is None:
            raise HTTPException(status_code=401, detail="Client site not found")
        input_dict = {
            "readqc-in": ["mgasmb", "rba"],
            "mgasmb-in": ["mganno"],
            "mganno-in": ["mgasmbgen"],
        }

        metadata_type = None

        if ingest.read_qc_analysis_activity_set:
            metadata_type = "readqc-in"

        if ingest.metagenome_assembly_activity_set:
            metadata_type = "mgasmb-in"

        if ingest.metagenome_annotation_activity_set:
            metadata_type = "mganno-in"

        drs_obj_doc = persist_content_and_get_drs_object(
            content=ingest.json(),
            filename=None,
            content_type="application/json",
            description=f"input metadata for {metadata_type} wf",
            id_ns=f"json-{metadata_type}-1.0.1",
        )

        for workflow_job in input_dict[metadata_type]:
            job_spec = {
                "workflow": {"id": f"{workflow_job}-1.0.1"},
                "config": {"object_id": drs_obj_doc["id"]},
            }

            run_config = merge(
                unfreeze(run_config_frozen__normal_env),
                {"ops": {"construct_jobs": {"config": {"base_jobs": [job_spec]}}}},
            )

            dagster_result: ExecuteInProcessResult = repo.get_job(
                "ensure_jobs"
            ).execute_in_process(run_config=run_config)

        return json.loads(json_util.dumps(drs_obj_doc))

    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail=e.details)
=======
#     # if site is None:
#     #     raise HTTPException(status_code=401, detail="Client site not found")

#     #     drs_obj_doc = persist_content_and_get_drs_object(
#     #         content=ingest.json(),
#     #         filename=None,
#     #         content_type="application/json",
#     #         description="input metadata for readqc-in wf",
#     #         id_ns="json-readqc-in",
#     #     )

#     #     doc_after = mdb.objects.find_one_and_update(
#     #         {"id": drs_obj_doc["id"]},
#     #         {"$set": {"types": ["readqc-in"]}},
#     #         return_document=ReturnDocument.AFTER,
#     #     )
#     #     return doc_after

#     # except DuplicateKeyError as e:
#     #     raise HTTPException(status_code=409, detail=e.details)
>>>>>>> 0474cb4 (feat(workflow_automation): add activities)
