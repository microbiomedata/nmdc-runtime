from fastapi import APIRouter, Depends
from jinja2 import Environment, PackageLoader, select_autoescape
from pymongo.database import Database as MongoDatabase
from starlette.responses import HTMLResponse
from toolz import merge

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db, activity_collection_names
from nmdc_runtime.api.endpoints.util import (
    find_resources,
    strip_oid,
    find_resources_spanning,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import (
    FindResponse,
    FindRequest,
    entity_attributes_to_index,
)

router = APIRouter()


@router.get(
    "/studies",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_studies(
    req: FindRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return find_resources(req, mdb, "study_set")


@router.get(
    "/studies/{study_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_study_by_id(
    study_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return strip_oid(raise404_if_none(mdb["study_set"].find_one({"id": study_id})))


@router.get(
    "/biosamples",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_biosamples(
    req: FindRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return find_resources(req, mdb, "biosample_set")


@router.get(
    "/biosamples/{sample_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_biosample_by_id(
    sample_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return strip_oid(raise404_if_none(mdb["biosample_set"].find_one({"id": sample_id})))


@router.get(
    "/data_objects",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_data_objects(
    req: FindRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return find_resources(req, mdb, "data_object_set")


@router.get(
    "/data_objects/{data_object_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_data_object_by_id(
    data_object_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return strip_oid(
        raise404_if_none(mdb["data_object_set"].find_one({"id": data_object_id}))
    )


@router.get(
    "/activities",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_activities(
    req: FindRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):

    return find_resources_spanning(req, mdb, activity_collection_names(mdb))


@router.get(
    "/activities/{activity_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_activity_by_id(
    activity_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    doc = None
    for name in activity_collection_names(mdb):
        doc = mdb[name].find_one({"id": activity_id})
        if doc is not None:
            return strip_oid(doc)

    return raise404_if_none(doc)


jinja_env = Environment(
    loader=PackageLoader("nmdc_runtime"), autoescape=select_autoescape()
)


def attr_index_sort_key(attr):
    return "_" if attr == "id" else attr


@router.get("/search", response_class=HTMLResponse)
def search_page(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    template = jinja_env.get_template("search.html")
    indexed_entity_attributes = merge(
        {n: {"id"} for n in activity_collection_names(mdb)},
        {
            coll: sorted(attrs | {"id"}, key=attr_index_sort_key)
            for coll, attrs in entity_attributes_to_index.items()
        },
    )
    html_content = template.render(
        activity_collection_names=activity_collection_names(mdb),
        indexed_entity_attributes=indexed_entity_attributes,
    )
    return HTMLResponse(content=html_content, status_code=200)
