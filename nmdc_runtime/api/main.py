import os
import re
from contextlib import asynccontextmanager
from importlib import import_module
from importlib.metadata import version
from typing import Annotated

import fastapi
import requests
import uvicorn
from bs4 import BeautifulSoup
from fastapi import APIRouter, FastAPI, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from setuptools_scm import get_version
from starlette import status
from starlette.responses import RedirectResponse, HTMLResponse, FileResponse

from nmdc_runtime.api.analytics import Analytics
from nmdc_runtime.util import (
    ensure_unique_id_indexes,
    REPO_ROOT_DIR,
)
from nmdc_runtime.api.core.auth import get_password_hash, ORCID_NMDC_CLIENT_ID
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
)
from nmdc_runtime.api.endpoints import (
    capabilities,
    find,
    jobs,
    metadata,
    nmdcschema,
    object_types,
    objects,
    operations,
    queries,
    runs,
    sites,
    triggers,
    users,
    workflows,
)
from nmdc_runtime.api.endpoints.util import BASE_URL_EXTERNAL
from nmdc_runtime.api.models.site import SiteClientInDB, SiteInDB
from nmdc_runtime.api.models.user import UserInDB
from nmdc_runtime.api.models.util import entity_attributes_to_index
from nmdc_runtime.api.v1.router import router_v1
from nmdc_runtime.minter.bootstrap import bootstrap as minter_bootstrap
from nmdc_runtime.minter.entrypoints.fastapi_app import router as minter_router

api_router = APIRouter()
api_router.include_router(users.router, tags=["users"])
api_router.include_router(operations.router, tags=["operations"])
api_router.include_router(sites.router, tags=["sites"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(objects.router, tags=["objects"])
api_router.include_router(capabilities.router, tags=["capabilities"])
api_router.include_router(triggers.router, tags=["triggers"])
api_router.include_router(workflows.router, tags=["workflows"])
api_router.include_router(object_types.router, tags=["object types"])
api_router.include_router(queries.router, tags=["queries"])
api_router.include_router(metadata.router, tags=["metadata"])
api_router.include_router(nmdcschema.router, tags=["metadata"])
api_router.include_router(find.router, tags=["find"])
api_router.include_router(runs.router, tags=["runs"])
api_router.include_router(router_v1, tags=["v1"])
api_router.include_router(minter_router, prefix="/pids", tags=["minter"])

tags_metadata = [
    {
        "name": "sites",
        "description": (
            """A site corresponds to a physical place that may participate in job execution.

A site may register data objects and capabilties with NMDC. It may claim jobs to execute, and it may
update job operations with execution info.

A site must be able to service requests for any data objects it has registered.

A site may expose a "put object" custom method for authorized users. This method facilitates an
operation to upload an object to the site and have the site register that object with the runtime
system.

"""
        ),
    },
    {
        "name": "users",
        "description": (
            """Endpoints for user identification.

Currently, accounts for use of the runtime API are created manually by system administrators.

          """
        ),
    },
    {
        "name": "workflows",
        "description": (
            """A workflow is a template for creating jobs.

Workflow jobs are typically created by the system via trigger associations between
workflows and object types. A workflow may also require certain capabilities of sites
in order for those sites to claim workflow jobs.
            """
        ),
    },
    {
        "name": "capabilities",
        "description": (
            """A workflow may require an executing site to have particular capabilities.

These capabilities go beyond the simple ability to access the data object resources registered with
the runtime system. Sites register their capabilities, and sites are only able to claim workflow
jobs if they are known to have the capabilities required by the workflow.

"""
        ),
    },
    {
        "name": "object types",
        "description": (
            """An object type is an object annotation that is useful for triggering workflows.

A data object may be annotated with one or more types, which in turn can be associated with
workflows through trigger resources.

The data-object type system may be used to trigger workflow jobs on a subset of data objects when a
new version of a workflow is deployed. This could be done by minting a special object type for the
occasion, annotating the subset of data objects with that type, and registering the association of
object type to workflow via a trigger resource.
            """
        ),
    },
    {
        "name": "triggers",
        "description": (
            """A trigger is an association between a workflow and a data object type.

When a data object is annotated with a type, perhaps shortly after object registration, the NMDC
Runtime will check, via trigger associations, for potential new jobs to create for any workflows.

            """
        ),
    },
    {
        "name": "jobs",
        "description": """A job is a resource that isolates workflow configuration from execution.

Rather than directly creating a workflow operation by supplying a workflow ID along with
configuration, NMDC creates a job that pairs a workflow with configuration. Then, a site can claim a
job ID, allowing the site to execute the intended workflow without additional configuration.

A job can have multiple executions, and a workflow's executions are precisely the executions of all
jobs created for that workflow.

A site that already has a compatible job execution result can preempt the unnecessary creation of a
job by pre-claiming it. This will return like a claim, and now the site can register known data
object inputs for the job without the risk of the runtime system creating a claimable job of the
pre-claimed type.

""",
    },
    {
        "name": "objects",
        "description": (
            """\
A [Data Repository Service (DRS)
object](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_datatypes)
represents content necessary for a workflow job to execute, and/or output from a job execution.

An object may be a *blob*, analogous to a file, or a *bundle*, analogous to a folder. Sites register
objects, and sites must ensure that these objects are accessible to the NMDC data broker.

An object may be associated with one or more object types, useful for triggering workflows.

"""
        ),
    },
    {
        "name": "operations",
        "description": """An operation is a resource for tracking the execution of a job.

When a job is claimed by a site for execution, an operation resource is created.

An operation is akin to a "promise" or "future" in that it should eventually resolve to either a
successful result, i.e. an execution resource, or to an error.

An operation is parameterized to return a result type, and a metadata type for storing progress
information, that are both particular to the job type.

Operations may be paused, resumed, and/or cancelled.

Operations may expire, i.e. not be stored indefinitely. In this case, it is recommended that
execution resources have longer lifetimes / not expire, so that information about successful results
of operations are available.
        """,
    },
    {
        "name": "queries",
        "description": (
            """A query is an operation (find, update, etc.) against the metadata store.

Metadata -- for studies, biosamples, omics processing, etc. -- is used by sites to execute jobs,
as the parameterization of job executions may depend not only on the content of data objects, but
also on objects' associated metadata.

Also, the function of many workflows is to extract or produce new metadata. Such metadata products
should be registered as data objects, and they may also be supplied by sites to the runtime system
as an update query (if the latter is not done, the runtime system will sense the new metadata and
issue an update query).

            """
        ),
    },
    {
        "name": "metadata",
        "description": """
The [metadata endpoints](https://api.microbiomedata.org/docs#/metadata) can be used to get and filter metadata from 
collection set types (including [studies](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/Study.html), 
[biosamples](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/Biosample.html), 
[data objects](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/DataObject.html), and 
[activities](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/Activity.html)).<br/>
 
The __metadata__ endpoints allow users to retrieve metadata from the data portal using the various GET endpoints 
that are slightly different than the __find__ endpoints, but some can be used similarly. As with the __find__  endpoints, 
parameters for the __metadata__ endpoints that do not have a red ___* required___ next to them are optional. <br/>

Unlike the compact syntax used in the __find__  endpoints, the syntax for the filter parameter of the metadata endpoints 
uses [MongoDB-like language querying](https://www.mongodb.com/docs/manual/tutorial/query-documents/). 
The applicable parameters of the __metadata__ endpoints, with acceptable syntax and examples, are in the table below.

<details>
<summary>More Details</summary>

| Parameter | Description | Syntax | Example |
| :---: | :-----------: | :-------: | :---: | 
| collection_name | The name of the collection to be queried. For a list of collection names please see the [Database class](https://microbiomedata.github.io/nmdc-schema/Database/) of the NMDC Schema | String | `biosample_set` |
| filter | Allows conditions to be set as part of the query, returning only results that satisfy the conditions | [MongoDB-like query language](https://www.mongodb.com/docs/manual/tutorial/query-documents/). All strings should be in double quotation marks. | `{"lat_lon.latitude": {"$gt": 45.0}, "ecosystem_category": "Plants"}` | 
| max_page_size | Specifies the maximum number of documents returned at a time | Integer | `25`
| page_token | Specifies the token of the page to return. If unspecified, the first page is returned. To retrieve a subsequent page, the value received as the `next_page_token` from the bottom of the previous results can be provided as a `page_token`. ![next_page_token](../_static/images/howto_guides/api_gui/metadata_page_token_param.png) | String | `nmdc:sys0ae1sh583`
| projection | Indicates the desired attributes to be included in the response. Helpful for trimming down the returned results | Comma-separated list of attributes that belong to the documents in the collection being queried | `name, ecosystem_type` |
| doc_id | The unique identifier of the item being requested. For example, the identifier of a biosample or an extraction | Curie e.g. `prefix:identifier` | `nmdc:bsm-11-ha3vfb58` |<br/>
<br/>
</details>        
        """,
    },
    {
        "name": "find",
        "description": """
The [find endpoints](https://api.microbiomedata.org/docs#/find:~:text=Find%20NMDC-,metadata,-entities.) are provided with 
NMDC metadata entities already specified - where metadata about [studies](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/Study.html), 
[biosamples](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/Biosample.html), 
[data objects](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/DataObject.html), and 
[activities](https://nmdc-documentation.readthedocs.io/en/latest/reference/metadata/Activity.html) can be retrieved using GET requests. 

Each endpoint is unique and requires the applicable attribute names to be known in order to structure a query in a meaningful way. 
Please note that endpoints with parameters that do not have a red ___* required___ label next to them are optional.<br/>

The applicable parameters of the ___find___ endpoints, with acceptable syntax and examples, are in the table below.

<details><summary>More Details</summary>

| Parameter | Description | Syntax | Example |
| :---: | :-----------: | :-------: | :---: |
| filter | Allows conditions to be set as part of the query, returning only results that satisfy the conditions | Comma separated string of attribute:value pairs. Can include comparison operators like >=, <=, <, and >. May use a `.search` after the attribute name to conduct a full text search of the field that are of type string. e.g. `attribute:value,attribute.search:value` | `ecosystem_category:Plants, lat_lon.latitude:>35.0` |
| search | Not yet implemented | Coming Soon | Not yet implemented |
| sort | Specifies the order in which the query returns the matching documents | Comma separated string of attribute:value pairs, where the value can be empty, `asc`, or `desc` (for ascending or descending order) e.g. `attribute` or `attribute:asc` or `attribute:desc`| `depth.has_numeric_value:desc, ecosystem_type` |
| page | Specifies the desired page number among the paginated results | Integer | `3` |
| per_page | Specifies the number of results returned per page. Maximum allowed is 2,000 | Integer | `50` |
| cursor | A bookmark for where a query can pick up where it has left off. To use cursor paging, set the `cursor` parameter to `*`. The results will include a `next_cursor` value in the response's `meta` object that can be used in the `cursor` parameter to retrieve the subsequent results ![next_cursor](../_static/images/howto_guides/api_gui/find_cursor.png) | String | `*` or `nmdc:sys0zr0fbt71` |
| group_by | Not yet implemented | Coming Soon | Not yet implemented |
| fields | Indicates the desired attributes to be included in the response. Helpful for trimming down the returned results | Comma-separated list of attributes that belong to the documents in the collection being queried | `name, ess_dive_datasets` |
| study_id | The unique identifier of a study | Curie e.g. `prefix:identifier` | `nmdc:sty-11-34xj1150` |
| sample_id | The unique identifier of a biosample | Curie e.g. `prefix:identifier` | `nmdc:bsm-11-w43vsm21` |
| data_object_id | The unique identifier of a data object | Curie e.g. `prefix:identifier` | `nmdc:dobj-11-7c6np651` |
| activity_id | The unique identifier for an NMDC workflow execution activity | Curie e.g. `prefix:identifier` | `nmdc:wfmgan-11-hvcnga50.1`|<br/>
<br/>
</details>

""",
    },
    {
        "name": "runs",
        "description": (
            "[WORK IN PROGRESS] Run simple jobs. "
            "For off-site job runs, keep the Runtime appraised of run events."
        ),
    },
]


def ensure_initial_resources_on_boot():
    """ensure these resources are loaded when (re-)booting the system."""
    mdb = get_mongo_db()

    collections = ["workflows", "capabilities", "object_types", "triggers"]
    for collection_name in collections:
        mdb[collection_name].create_index("id", unique=True)
        collection_boot = import_module(f"nmdc_runtime.api.boot.{collection_name}")

        for model in collection_boot.construct():
            doc = model.model_dump()
            mdb[collection_name].replace_one({"id": doc["id"]}, doc, upsert=True)

    username = os.getenv("API_ADMIN_USER")
    admin_ok = mdb.users.count_documents(({"username": username})) > 0
    if not admin_ok:
        mdb.users.replace_one(
            {"username": username},
            UserInDB(
                username=username,
                hashed_password=get_password_hash(os.getenv("API_ADMIN_PASS")),
                site_admin=[os.getenv("API_SITE_ID")],
            ).model_dump(exclude_unset=True),
            upsert=True,
        )
        mdb.users.create_index("username", unique=True)

    site_id = os.getenv("API_SITE_ID")
    runtime_site_ok = mdb.sites.count_documents(({"id": site_id})) > 0
    if not runtime_site_ok:
        client_id = os.getenv("API_SITE_CLIENT_ID")
        mdb.sites.replace_one(
            {"id": site_id},
            SiteInDB(
                id=site_id,
                clients=[
                    SiteClientInDB(
                        id=client_id,
                        hashed_secret=get_password_hash(
                            os.getenv("API_SITE_CLIENT_SECRET")
                        ),
                    )
                ],
            ).model_dump(),
            upsert=True,
        )

    ensure_unique_id_indexes(mdb)

    # No two object documents can have the same checksum of the same type.
    mdb.objects.create_index(
        [("checksums.type", 1), ("checksums.checksum", 1)], unique=True
    )

    # Minting resources
    minter_bootstrap()


def ensure_attribute_indexes():
    mdb = get_mongo_db()
    for collection_name, index_specs in entity_attributes_to_index.items():
        for spec in index_specs:
            if not isinstance(spec, str):
                raise ValueError(
                    "only supports basic single-key ascending index specs at this time."
                )

            mdb[collection_name].create_index([(spec, 1)], name=spec, background=True)


def ensure_default_api_perms():
    db = get_mongo_db()
    if db["_runtime.api.allow"].count_documents({}):
        return

    allowed = {
        "/metadata/changesheets:submit": [
            "admin",
        ],
        "/queries:run(query_cmd:DeleteCommand)": [
            "admin",
        ],
        "/metadata/json:submit": [
            "admin",
        ],
    }
    for doc in [
        {"username": username, "action": action}
        for action, usernames in allowed.items()
        for username in usernames
    ]:
        db["_runtime.api.allow"].replace_one(doc, doc, upsert=True)
        db["_runtime.api.allow"].create_index("username")
        db["_runtime.api.allow"].create_index("action")


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_initial_resources_on_boot()
    ensure_attribute_indexes()
    ensure_default_api_perms()
    yield


@api_router.get("/")
async def root():
    return RedirectResponse(
        BASE_URL_EXTERNAL + "/docs",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@api_router.get("/version")
async def get_versions():
    return {
        "nmdc-runtime": get_version(),
        "fastapi": fastapi.__version__,
        "nmdc-schema": version("nmdc_schema"),
    }


app = FastAPI(
    title="NMDC Runtime API",
    version=get_version(),
    description=(
        "The NMDC Runtime API, via on-demand functions "
        "and via schedule-based and sensor-based automation, "
        "supports validation and submission of metadata, as well as "
        "orchestration of workflow execution activities."
        "\n\n"
        "Dependency versions:\n\n"
        f'nmdc-schema={version("nmdc_schema")}\n\n'
        "<a href='https://microbiomedata.github.io/nmdc-runtime/'>Documentation</a>\n\n"
        '<img src="/static/ORCIDiD_icon128x128.png" height="18" width="18"/> '
        f'<a href="https://orcid.org/oauth/authorize?client_id={ORCID_NMDC_CLIENT_ID}'
        "&response_type=code&scope=openid&"
        f'redirect_uri={BASE_URL_EXTERNAL}/orcid_code">Login with ORCiD</a>'
        " (note: this link is static; if you are logged in, you will see a 'locked' lock icon"
        " in the below-right 'Authorized' button.)"
    ),
    openapi_tags=tags_metadata,
    lifespan=lifespan,
    docs_url=None,
)
app.include_router(api_router)


app.add_middleware(
    CORSMiddleware,
    # Allow requests from client-side web apps hosted in local development environments, on microbiomedata.org, and on GitHub Pages.
    allow_origin_regex=r"(http://localhost:\d+)|(https://.+?\.microbiomedata\.org)|(https://microbiomedata\.github\.io)",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(Analytics)
app.mount(
    "/static",
    StaticFiles(directory=REPO_ROOT_DIR.joinpath("nmdc_runtime/static/")),
    name="static",
)


@app.get("/favicon.ico")
async def favicon():
    return FileResponse("static/favicon.ico")


@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html(
    user_id_token: Annotated[str | None, Cookie()] = None,
):
    access_token = None
    if user_id_token:
        # get bearer token
        rv = requests.post(
            url=f"{BASE_URL_EXTERNAL}/token",
            data={
                "client_id": user_id_token,
                "client_secret": "",
                "grant_type": "client_credentials",
            },
            headers={
                "Content-type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )
        if rv.status_code != 200:
            rv.reason = rv.text
            rv.raise_for_status()
        access_token = rv.json()["access_token"]

    swagger_ui_parameters = {"withCredentials": True}
    onComplete = ""
    if access_token is not None:
        onComplete += f"""ui.preauthorizeApiKey(<double-quote>bearerAuth</double-quote>, <double-quote>{access_token}</double-quote>); """
    if os.getenv("INFO_BANNER_INNERHTML"):
        info_banner_innerhtml = os.getenv("INFO_BANNER_INNERHTML")
        onComplete += f"""banner = document.createElement(<double-quote>section</double-quote>); banner.classList.add(<double-quote>nmdc-info-banner</double-quote>); banner.classList.add(<double-quote>block</double-quote>); banner.classList.add(<double-quote>col-12</double-quote>); banner.innerHTML = `{info_banner_innerhtml.replace('"', '<double-quote>')}`; document.querySelector(<double-quote>.information-container</double-quote>).prepend(banner); """
    if onComplete:
        swagger_ui_parameters.update(
            {
                "onComplete": f"""<unquote-safe>() => {{ {onComplete} }}</unquote-safe>""",
            }
        )
    response = get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title,
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.9.0/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.9.0/swagger-ui.css",
        swagger_favicon_url="/static/favicon.ico",
        swagger_ui_parameters=swagger_ui_parameters,
    )
    content = (
        response.body.decode()
        .replace('"<unquote-safe>', "")
        .replace('</unquote-safe>"', "")
        .replace("<double-quote>", '"')
        .replace("</double-quote>", '"')
        .replace(
            "</head>",
            "<style>.nmdc-info-banner { padding: 1em; background-color: #448aff1a; border: .075rem solid #448aff; }</style></head>",
        )
    )
    return HTMLResponse(content=content)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
