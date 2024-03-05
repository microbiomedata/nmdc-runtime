"""Under embargo due to E999 SyntaxError"""

# """Module"""
# from fastapi import APIRouter, Depends, HTTPException
# from nmdc_runtime.api.models.site import Site, get_current_client_site
# from pymongo.errors import DuplicateKeyError
# from starlette import status
#
# from components.nmdc_runtime.workflow_execution_activity import ActivitySet
#
# router = APIRouter(prefix="/activities", tags=["workflow_execution_activities"])
#
#
# @router.post(
#     activity_set: ActivitySet,
#     status_code=status.HTTP_201_CREATED,
# )
# async def post_l(
#     site: Site = Depends(get_current_client_site),
# ) -> None:
#     """Docs"""
#     try:
#
#         if site is None:
#             raise HTTPException(status_code=401, detail="Client site not found")
#
#     #     drs_obj_doc = persist_content_and_get_drs_object(
#     #         content=ingest.json(),
#     #         filename=None,
#     #         content_type="application/json",
#     #         description="input metadata for readqc-in wf",
#     #         id_ns="json-readqc-in",
#     #     )
#
#     #     doc_after = mdb.objects.find_one_and_update(
#     #         {"id": drs_obj_doc["id"]},
#     #         {"$set": {"types": ["readqc-in"]}},
#     #         return_document=ReturnDocument.AFTER,
#     #     )
#     #     return doc_after
#
#     except DuplicateKeyError as e:
#         raise HTTPException(status_code=409, detail=e.details)
