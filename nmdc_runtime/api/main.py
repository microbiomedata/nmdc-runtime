import os
from contextlib import asynccontextmanager
from html import escape
from importlib import import_module
from importlib.metadata import version
from typing import Annotated
from pathlib import Path

import fastapi
import requests
import uvicorn
from fastapi import APIRouter, FastAPI, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from starlette import status
from starlette.responses import RedirectResponse, HTMLResponse, FileResponse
from refscan.lib.helpers import get_collection_names_from_schema
from scalar_fastapi import get_scalar_api_reference

from nmdc_runtime import config
from nmdc_runtime.api.analytics import Analytics
from nmdc_runtime.api.middleware import PyinstrumentMiddleware
from nmdc_runtime.config import IS_SCALAR_ENABLED
from nmdc_runtime.util import (
    decorate_if,
    get_allowed_references,
    ensure_unique_id_indexes,
    REPO_ROOT_DIR,
    nmdc_schema_view,
)
from nmdc_runtime.api.core.auth import (
    get_password_hash,
    ORCID_NMDC_CLIENT_ID,
    ORCID_BASE_URL,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
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
from nmdc_runtime.api.openapi import ordered_tag_descriptors, make_api_description
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
api_router.include_router(minter_router, prefix="/pids", tags=["minter"])


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


def ensure_type_field_is_indexed():
    r"""
    Ensures that each schema-described collection has an index on its `type` field.
    """

    mdb = get_mongo_db()
    schema_view = nmdc_schema_view()
    for collection_name in get_collection_names_from_schema(schema_view):
        mdb.get_collection(collection_name).create_index("type", background=True)


def ensure_attribute_indexes():
    r"""
    Ensures that the MongoDB collection identified by each key (i.e. collection name) in the
    `entity_attributes_to_index` dictionary, has an index on each field identified by the value
    (i.e. set of field names) associated with that key.

    Example dictionary (notice each item's value is a _set_, not a _dict_):
    ```
    {
        "coll_name_1": {"field_name_1"},
        "coll_name_2": {"field_name_1", "field_name_2"},
    }
    ```
    """

    mdb = get_mongo_db()
    for collection_name, index_specs in entity_attributes_to_index.items():
        for spec in index_specs:
            if not isinstance(spec, str):
                raise ValueError(
                    "only supports basic single-key ascending index specs at this time."
                )

            mdb[collection_name].create_index([(spec, 1)], name=spec, background=True)


def ensure_default_api_perms():
    """
    Ensures that specific users (currently only "admin") are allowed to perform
    specific actions, and creates MongoDB indexes to speed up allowance queries.

    Note: If a MongoDB index already exists, the call to `create_index` does nothing.
    """

    db = get_mongo_db()
    if db["_runtime.api.allow"].count_documents({}):
        return

    allowances = {
        "/metadata/changesheets:submit": [
            "admin",
        ],
        "/queries:run(query_cmd:DeleteCommand)": [
            "admin",
        ],
        "/queries:run(query_cmd:AggregateCommand)": [
            "admin",
        ],
        "/metadata/json:submit": [
            "admin",
        ],
    }
    for doc in [
        {"username": username, "action": action}
        for action, usernames in allowances.items()
        for username in usernames
    ]:
        db["_runtime.api.allow"].replace_one(doc, doc, upsert=True)
        db["_runtime.api.allow"].create_index("username")
        db["_runtime.api.allow"].create_index("action")


@asynccontextmanager
async def lifespan(app: FastAPI):
    r"""
    Prepares the application to receive requests.

    From the [FastAPI documentation](https://fastapi.tiangolo.com/advanced/events/#lifespan-function):
    > You can define logic (code) that should be executed before the application starts up. This means that
    > this code will be executed once, before the application starts receiving requests.
    """
    ensure_initial_resources_on_boot()
    ensure_attribute_indexes()
    ensure_type_field_is_indexed()
    ensure_default_api_perms()

    # Invoke a function—thereby priming its memoization cache—in order to speed up all future invocations.
    get_allowed_references()  # we ignore the return value here

    yield


@api_router.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(
        BASE_URL_EXTERNAL + "/docs",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@api_router.get("/version")
async def get_versions():
    return {
        "nmdc-runtime": version("nmdc_runtime"),
        "fastapi": fastapi.__version__,
        "nmdc-schema": version("nmdc_schema"),
    }


app = FastAPI(
    title="NMDC Runtime API",
    version=version("nmdc_runtime"),
    description=make_api_description(
        schema_version=version("nmdc_schema"),
        orcid_login_url=f"{ORCID_BASE_URL}/oauth/authorize?client_id={ORCID_NMDC_CLIENT_ID}&response_type=code&scope=openid&redirect_uri={BASE_URL_EXTERNAL}/orcid_code",
    ),
    openapi_tags=ordered_tag_descriptors,
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

if config.IS_PROFILING_ENABLED:
    app.add_middleware(PyinstrumentMiddleware)

# Note: Here, we are mounting a `StaticFiles` instance (which is bound to the directory that
#       contains static files) as a "sub-application" of the main FastAPI application. This
#       makes the contents of that directory be accessible under the `/static` URL path.
#       Reference: https://fastapi.tiangolo.com/tutorial/static-files/
static_files_path: Path = REPO_ROOT_DIR.joinpath("nmdc_runtime/static/")
app.mount("/static", StaticFiles(directory=static_files_path), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    r"""Returns the application's favicon."""
    favicon_path = static_files_path / "favicon.ico"
    return FileResponse(favicon_path)


@decorate_if(condition=IS_SCALAR_ENABLED)(app.get("/scalar", include_in_schema=False))
async def get_scalar_html():
    r"""
    Returns the HTML markup for an interactive API docs web page
    (alternative to Swagger UI) powered by Scalar.
    """
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title="NMDC Runtime API",
    )


@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html(
    user_id_token: Annotated[str | None, Cookie()] = None,
):
    r"""Returns the HTML markup for an interactive API docs web page powered by Swagger UI.

    If the `user_id_token` cookie is present and not empty, this function will send its value to
    the `/token` endpoint in an attempt to get an access token. If it gets one, this function will
    inject that access token into the web page so Swagger UI will consider the user to be logged in.

    Reference: https://fastapi.tiangolo.com/tutorial/cookie-params/
    """
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

    # Note: Setting `persistAuthorization` to `True` makes it so a logged-in user remains logged-in
    #       even after reloading the web page (or leaving the website and coming back to it later).
    # Reference: https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/#parameters
    swagger_ui_parameters: dict = {
        "withCredentials": True,
        "persistAuthorization": True,
    }
    onComplete = ""
    if access_token is not None:
        onComplete += f"ui.preauthorizeApiKey('bearerAuth', '{access_token}');"
    if os.getenv("INFO_BANNER_INNERHTML"):
        info_banner_innerhtml = os.getenv("INFO_BANNER_INNERHTML")
        onComplete += f"""
            banner = document.createElement('section');
            banner.classList.add('nmdc-info', 'nmdc-info-banner', 'block', 'col-12');
            banner.innerHTML = `{info_banner_innerhtml.replace('"', '<double-quote>')}`;
            document.querySelector('.information-container').prepend(banner);
        """.replace(
            "\n", " "
        )
    # Note: The `nmdcInit` JavaScript event is a custom event we use to trigger anything that is listening for it.
    #       Reference: https://developer.mozilla.org/en-US/docs/Web/Events/Creating_and_triggering_events
    swagger_ui_parameters.update(
        {
            "onComplete": f"""<unquote-safe>() => {{ {onComplete}; dispatchEvent(new Event('nmdcInit')); }}</unquote-safe>""",
        }
    )
    # Note: Consider using a "custom layout" instead of injecting HTML snippets via Python.
    #       Reference: https://github.com/swagger-api/swagger-ui/blob/master/docs/customization/custom-layout.md
    response = get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title,
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_favicon_url="/static/favicon.ico",
        swagger_ui_parameters=swagger_ui_parameters,
    )
    assets_dir_path = Path(__file__).parent / "swagger_ui" / "assets"
    style_css: str = Path(assets_dir_path / "style.css").read_text()
    script_js: str = Path(assets_dir_path / "script.js").read_text()
    content = (
        response.body.decode()
        .replace('"<unquote-safe>', "")
        .replace('</unquote-safe>"', "")
        .replace("<double-quote>", '"')
        .replace("</double-quote>", '"')
        # Inject an HTML element containing the access token (or an empty string, if there is no access token)
        # as the value of an HTML5 data-* attribute. This makes the access token (or the empty string)
        # available to JavaScript code running on the web page (e.g., `swagger_ui/assets/script.js`).
        .replace(
            "</head>",
            f"""
            </head>
            <div
                id="nmdc-access-token"
                data-token="{escape(access_token if access_token is not None else '')}"
                style="display: none"
            ></div>
            """,
        )
        # Inject a custom CSS stylesheet immediately before the closing `</head>` tag.
        .replace("</head>", f"<style>\n{style_css}\n</style>\n</head>")
        # Inject a custom JavaScript script immediately before the closing `</body>` tag.
        .replace("</body>", f"<script>\n{script_js}\n</script>\n</body>")
    )
    return HTMLResponse(content=content)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
