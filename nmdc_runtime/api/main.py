import os
import logging
from contextlib import asynccontextmanager
from html import escape
from importlib import import_module
from importlib.metadata import version
from typing import Annotated
from pathlib import Path

import fastapi
import requests
import sentry_sdk
import uvicorn
from fastapi import APIRouter, FastAPI, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from refscan.lib.helpers import get_collection_names_from_schema
from scalar_fastapi import get_scalar_api_reference
from starlette import status
from starlette.responses import RedirectResponse, HTMLResponse, FileResponse

from nmdc_runtime import config
from nmdc_runtime.api.models.wfe_file_stages import WorkflowFileStagingCollectionName
from nmdc_runtime.api.analytics import Analytics
from nmdc_runtime.api.middleware import PyinstrumentMiddleware
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
    allowances,
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
    wf_file_staging,
)
from nmdc_runtime.api.endpoints.util import BASE_URL_EXTERNAL
from nmdc_runtime.api.models.site import SiteClientInDB, SiteInDB
from nmdc_runtime.api.models.user import UserInDB
from nmdc_runtime.api.models.util import entity_attributes_to_index
from nmdc_runtime.api.models.allowance import AllowanceAction
from nmdc_runtime.api.openapi import (
    OpenAPITag,
    ordered_tag_descriptors,
    make_api_description,
)
from nmdc_runtime.api.swagger_ui.swagger_ui import base_swagger_ui_parameters
from nmdc_runtime.minter.bootstrap import bootstrap as minter_bootstrap
from nmdc_runtime.minter.entrypoints.fastapi_app import router as minter_router


# If the app is configured to use Sentry, initialize the Sentry SDK now.
#
# Note: The FastAPI integration will be automatically enabled, since we list `fastapi`
#       as a dependency in `pyproject.toml`. If we eventually decide to configure the
#       integration differently from its defaults, we can follow the instructions at:
#       https://docs.sentry.io/platforms/python/integrations/fastapi/#configure
#
if config.IS_SENTRY_ENABLED and len(config.SENTRY_DSN.strip()) > 0:
    logging.info(
        f"Initializing Sentry SDK (Sentry environment: {config.SENTRY_ENVIRONMENT})."
    )
    logging.debug(f"Sentry traces sample rate: {config.SENTRY_TRACES_SAMPLE_RATE}")
    logging.debug(f"Sentry profiles sample rate: {config.SENTRY_PROFILES_SAMPLE_RATE}")
    sentry_sdk.init(
        dsn=config.SENTRY_DSN,
        environment=config.SENTRY_ENVIRONMENT,
        traces_sample_rate=config.SENTRY_TRACES_SAMPLE_RATE,
        profiles_sample_rate=config.SENTRY_PROFILES_SAMPLE_RATE,
    )


api_router = APIRouter()
api_router.include_router(find.router, tags=[OpenAPITag.METADATA_ACCESS.value])
api_router.include_router(nmdcschema.router, tags=[OpenAPITag.METADATA_ACCESS.value])
api_router.include_router(queries.router, tags=[OpenAPITag.METADATA_ACCESS.value])
api_router.include_router(metadata.router, tags=[OpenAPITag.METADATA_ACCESS.value])
api_router.include_router(sites.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(workflows.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(capabilities.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(object_types.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(triggers.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(jobs.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(objects.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(operations.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(runs.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(minter_router, prefix="/pids", tags=[OpenAPITag.MINTER.value])
api_router.include_router(users.router, tags=[OpenAPITag.USERS.value])
api_router.include_router(wf_file_staging.router, tags=[OpenAPITag.WORKFLOWS.value])
api_router.include_router(allowances.router, tags=[OpenAPITag.USERS.value])


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


def ensure_allowance_is_indexed():
    """
    Creates indexes in the `_runtime.api.allow` collection; specifically, of
    the 'username' field, of the 'action' field, and of the combination of
    those two fields.
    """
    mdb = get_mongo_db()
    mdb["_runtime.api.allow"].create_index("username")
    mdb["_runtime.api.allow"].create_index("action")
    # ensure unique composite index on (username, action)
    mdb["_runtime.api.allow"].create_index(
        [("username", 1), ("action", 1)], unique=True
    )


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


def ensure_globus_tasks_id_is_indexed():
    """
    Ensures that the `wf_file_staging.globus_tasks` collection has an index on its `task_id` field and that the index is unique.
    """

    mdb = get_mongo_db()
    mdb["wf_file_staging.globus_tasks"].create_index(
        "task_id", background=True, unique=True
    )


def ensure_jgi_samples_id_is_indexed():
    """
    Ensures that the `wf_file_staging.jgi_samples` collection has an index on its `jdp_file_id` field and that the index is unique.
    """

    mdb = get_mongo_db()
    mdb["wf_file_staging.jgi_samples"].create_index(
        "jdp_file_id", background=True, unique=True
    )


def ensure_sequencing_project_name_is_indexed():
    """
    Ensures that the `wf_file_staging.sequencing_projects` collection has an index on its `sequencing_project_name` field and that the index is unique.
    """

    mdb = get_mongo_db()
    mdb[WorkflowFileStagingCollectionName.JGI_SEQUENCING_PROJECTS.value].create_index(
        "sequencing_project_name", background=True, unique=True
    )


def ensure_default_api_perms():
    """
    Ensures that specific users (currently only "admin") are allowed to perform
    specific actions, and creates MongoDB indexes to speed up allowance queries.
    Note: If a MongoDB index already exists, the call to `create_index` does nothing.
    """
    db = get_mongo_db()
    if db["_runtime.api.allow"].count_documents({}):
        return

    default_users = ["admin"]

    for action in AllowanceAction:
        for username in default_users:
            doc = {"username": username, "action": action.value}
            db["_runtime.api.allow"].replace_one(doc, doc, upsert=True)


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
    ensure_globus_tasks_id_is_indexed()
    ensure_sequencing_project_name_is_indexed()
    ensure_jgi_samples_id_is_indexed()
    ensure_allowance_is_indexed()
    # Invoke a function—thereby priming its memoization cache—in order to speed up all future invocations.
    get_allowed_references()  # we ignore the return value here

    yield


@api_router.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(
        BASE_URL_EXTERNAL + "/docs",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@api_router.get("/version", tags=[OpenAPITag.SYSTEM_ADMINISTRATION.value])
async def get_versions():
    return {
        "nmdc-runtime": version("nmdc_runtime"),
        "fastapi": fastapi.__version__,
        "nmdc-schema": version("nmdc_schema"),
    }


# Build an ORCID Login URL for the Swagger UI page, based upon some environment variables.
orcid_login_url = f"{ORCID_BASE_URL}/oauth/authorize?client_id={ORCID_NMDC_CLIENT_ID}&response_type=code&scope=openid&redirect_uri={BASE_URL_EXTERNAL}/orcid_code"


app = FastAPI(
    title="NMDC Runtime API",
    version=version("nmdc_runtime"),
    description=make_api_description(
        api_version=version("nmdc_runtime"), schema_version=version("nmdc_schema")
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


@decorate_if(condition=config.IS_SCALAR_ENABLED)(
    app.get("/scalar", include_in_schema=False)
)
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
    swagger_ui_parameters = base_swagger_ui_parameters.copy()
    # Note: The `nmdcInit` JavaScript event is a custom event we use to trigger anything that is listening for it.
    #       Reference: https://developer.mozilla.org/en-US/docs/Web/Events/Creating_and_triggering_events
    swagger_ui_parameters.update(
        {
            "onComplete": f"""<unquote-safe>() => {{ {onComplete}; dispatchEvent(new Event('nmdcInit')); }}</unquote-safe>""",
        }
    )
    # Pin the Swagger UI version to avoid breaking changes (to our own JavaScript code that depends on Swagger UI internals).
    # Note: version `5.29.4` was released on October 10, 2025.
    pinned_swagger_ui_version = "5.29.4"
    swagger_ui_css_url = f"https://cdn.jsdelivr.net/npm/swagger-ui-dist@{pinned_swagger_ui_version}/swagger-ui.css"
    swagger_ui_js_url = f"https://cdn.jsdelivr.net/npm/swagger-ui-dist@{pinned_swagger_ui_version}/swagger-ui-bundle.js"
    response = get_swagger_ui_html(
        swagger_css_url=swagger_ui_css_url,
        swagger_js_url=swagger_ui_js_url,
        openapi_url=app.openapi_url,
        title=app.title,
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_favicon_url="/static/favicon.ico",
        swagger_ui_parameters=swagger_ui_parameters,
    )
    assets_dir_path = Path(__file__).parent / "swagger_ui" / "assets"
    style_css: str = Path(assets_dir_path / "style.css").read_text()
    script_js: str = Path(assets_dir_path / "script.js").read_text()
    ellipses_button_js: str = Path(assets_dir_path / "EllipsesButton.js").read_text()
    endpoint_search_widget_js: str = Path(
        assets_dir_path / "EndpointSearchWidget.js"
    ).read_text()
    content = (
        response.body.decode()
        .replace('"<unquote-safe>', "")
        .replace('</unquote-safe>"', "")
        .replace("<double-quote>", '"')
        .replace("</double-quote>", '"')
        # TODO: Consider using a "custom layout" implemented as a React component.
        #       Reference: https://github.com/swagger-api/swagger-ui/blob/master/docs/customization/custom-layout.md
        #
        #       Note: Custom layouts are specified via the Swagger UI parameter named `layout`, whose value identifies
        #             a component that is specified via the Swagger UI parameter named `plugins`. The Swagger UI
        #             JavaScript code expects each item in the `plugins` array to be a JavaScript function,
        #             but FastAPI's `get_swagger_ui_html` function serializes each parameter's value into JSON,
        #             preventing us from specifying a JavaScript function as a value in the `plugins` array.
        #
        #             As a workaround, we could use the string `replace`-ment technique shown below to put the literal
        #             JavaScript characters into place in the final HTML document. Using that approach, I _have_ been
        #             able to display a custom layout (a custom React component), but I have _not_ been able to get
        #             that custom layout to display Swagger UI's `BaseLayout` component (which includes the core
        #             Swagger UI functionality). That's a deal breaker.
        #
        .replace(r'"{{ NMDC_SWAGGER_UI_PARAMETERS_PLUGINS_PLACEHOLDER }}"', r"[]")
        # Inject HTML elements containing data that can be read via JavaScript (e.g., `swagger_ui/assets/script.js`).
        # Note: We escape the values here so they can be safely used as HTML attribute values.
        .replace(
            "</head>",
            f"""
            </head>
            <div
                id="nmdc-access-token"
                data-token="{escape(access_token if access_token is not None else '')}"
                style="display: none"
            ></div>
            <div
                id="nmdc-orcid-login-url"
                data-url="{escape(orcid_login_url)}"
                style="display: none"
            ></div>
            """,
            1,
        )
        # Inject a custom CSS stylesheet immediately before the closing `</head>` tag.
        .replace(
            "</head>",
            f"""
                <style>
                    {style_css}
                </style>
            </head>
            """,
            1,
        )
        # Inject custom JavaScript scripts immediately before the closing `</body>` tag.
        .replace(
            "</body>",
            f"""
                <script>
                    {ellipses_button_js}
                    {endpoint_search_widget_js}
                    {script_js}
                </script>
            </body>
            """,
            1,
        )
    )
    return HTMLResponse(content=content)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
