from operator import itemgetter

from fastapi import APIRouter, Depends, Form
from jinja2 import Environment, PackageLoader, select_autoescape
from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict
from pymongo.database import Database as MongoDatabase
from starlette.responses import HTMLResponse
from toolz import merge, assoc_in

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db, activity_collection_names
from nmdc_runtime.api.endpoints.util import (
    find_resources,
    strip_oid,
    find_resources_spanning,
    pipeline_find_resources,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import (
    FindResponse,
    FindRequest,
    entity_attributes_to_index,
    PipelineFindRequest,
    PipelineFindResponse,
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


def documentation_links(jsonschema_dict, collection_names):
    rv = {"Activity": []}
    for cn in collection_names:
        last_part = jsonschema_dict["$defs"]["Database"]["properties"][cn]["items"][
            "$ref"
        ].split("/")[-1]
        entity_attrs = list(
            get_nmdc_jsonschema_dict()["$defs"][last_part]["properties"]
        )
        if last_part in ("Biosample", "Study", "DataObject"):
            assoc_path = [cn]
        else:
            assoc_path = ["activity_set", cn]
        rv = assoc_in(
            rv,
            assoc_path,
            {
                "collection_name": cn,
                "entity_url": "https://microbiomedata.github.io/nmdc-schema/"
                + last_part,
                "entity_name": last_part,
                "entity_attrs": sorted(
                    [
                        {
                            "url": f"https://microbiomedata.github.io/nmdc-schema/{a}",
                            "attr_name": a,
                        }
                        for a in entity_attrs
                    ],
                    key=itemgetter("attr_name"),
                ),
            },
        )

    return rv


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
    doc_links = documentation_links(
        get_nmdc_jsonschema_dict(),
        (
            list(activity_collection_names(mdb))
            + ["biosample_set", "study_set", "data_object_set"]
        ),
    )
    html_content = template.render(
        activity_collection_names=sorted(activity_collection_names(mdb)),
        indexed_entity_attributes=indexed_entity_attributes,
        doc_links=doc_links,
    )
    return HTMLResponse(content=html_content, status_code=200)


@router.post(
    "/pipeline_search",
    response_model=PipelineFindResponse,
    response_model_exclude_unset=True,
)
def pipeline_search(
    req: PipelineFindRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    return pipeline_find_resources(req, mdb)


@router.post(
    "/pipeline_search_form",
    response_model=PipelineFindResponse,
    response_model_exclude_unset=True,
)
def pipeline_search(
    pipeline_spec: str = Form(...),
    description: str = Form(...),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    req = PipelineFindRequest(pipeline_spec=pipeline_spec, description=description)
    return pipeline_find_resources(req, mdb)


@router.get("/pipeline_search", response_class=HTMLResponse)
def pipeline_search(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    template = jinja_env.get_template("pipeline_search.html")
    html_content = template.render()
    return HTMLResponse(content=html_content, status_code=200)
