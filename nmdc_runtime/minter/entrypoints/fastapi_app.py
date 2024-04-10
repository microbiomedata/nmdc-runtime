import os
import traceback
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, FastAPI, Cookie
from fastapi.openapi.docs import get_swagger_ui_html
from pymongo.database import Database as MongoDatabase
import requests
from setuptools_scm import get_version
from starlette import status
from starlette.responses import RedirectResponse, HTMLResponse
from starlette.staticfiles import StaticFiles

from nmdc_runtime.api.analytics import Analytics
from nmdc_runtime.api.core.auth import ORCID_NMDC_CLIENT_ID
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.main import (
    ensure_initial_resources_on_boot,
)


from nmdc_runtime.api.models.site import get_current_client_site, Site
from nmdc_runtime.minter.adapters.repository import MongoIDStore, MinterError
from nmdc_runtime.api.endpoints import users
from nmdc_runtime.minter.bootstrap import bootstrap as minter_bootstrap
from nmdc_runtime.minter.config import minting_service_id
from nmdc_runtime.minter.domain.model import (
    Identifier,
    AuthenticatedMintingRequest,
    MintingRequest,
    Entity,
    ResolutionRequest,
    BindingRequest,
    AuthenticatedBindingRequest,
    AuthenticatedDeleteRequest,
    DeleteRequest,
)
from nmdc_runtime.util import REPO_ROOT_DIR

BASE_URL_EXTERNAL = os.getenv("MINTER_HOST_EXTERNAL")

router = APIRouter()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_initial_resources_on_boot()
    minter_bootstrap()
    yield


app = FastAPI(
    title="NMDC Minter API",
    version=get_version(),
    description=(
        "The NMDC Minter API requires authentication via the `clientCredentials` flow, "
        "i.e. via `client_id` and `client_secret`."
        "\n\n"
        "Dependency versions:\n\n"
        f'nmdc-schema={version("nmdc_schema")}\n\n'
    ),
    lifespan=lifespan,
    docs_url="/minter_docs",
)
# FIXME need "/pids" prefix for user stuff, as minter api will be at api.microbiomedata.org
app.include_router(users.router, tags=["users"])

app.add_middleware(Analytics, collection="_minter.analytics")
app.mount(
    "/static",
    StaticFiles(directory=REPO_ROOT_DIR.joinpath("nmdc_runtime/static/")),
    name="static",
)


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
    if access_token is not None:
        swagger_ui_parameters.update(
            {
                "onComplete": f"""<unquote-safe>() => {{ ui.preauthorizeApiKey(<double-quote>bearerAuth</double-quote>, <double-quote>{access_token}</double-quote>) }}</unquote-safe>""",
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
    )
    return HTMLResponse(content=content)


app.add_api_route(
    "/minter_docs", custom_swagger_ui_html, methods=["GET"], include_in_schema=False
)


@app.get("/")
async def root():
    return RedirectResponse(
        BASE_URL_EXTERNAL + "/minter_docs",
        status_code=status.HTTP_303_SEE_OTHER,
    )


service = Entity(id=minting_service_id())


@router.post("/mint")
def mint_ids(
    req_mint: AuthenticatedMintingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> list[str]:
    """Mint one or more (typed) persistent identifiers."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        minted = s.mint(
            MintingRequest(
                service=service,
                requester=requester,
                **req_mint.model_dump(),
            )
        )
        return [d.id for d in minted]
    except MinterError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=traceback.format_exc(),
        )


@router.get("/resolve/{id_name}")
def resolve_id(
    id_name: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> Identifier:
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    id_ = s.resolve(
        ResolutionRequest(service=service, requester=requester, id_name=id_name)
    )
    return raise404_if_none(id_)


@router.post("/bind")
def bind(
    req_bind: AuthenticatedBindingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> Identifier:
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        id_ = s.bind(
            BindingRequest(
                service=service,
                requester=requester,
                id_name=req_bind.id_name,
                metadata_record=req_bind.metadata_record,
            )
        )
        return raise404_if_none(id_)
    except Exception as e:
        if str(e) == f"ID {req_bind.id_name} is unknown":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=traceback.format_exc(),
            )


@router.post("/delete")
def delete(
    req_del: AuthenticatedDeleteRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        id_ = s.delete(
            DeleteRequest(
                service=service,
                requester=requester,
                id_name=req_del.id_name,
            )
        )
    except Exception as e:
        if str(e) == f"ID {req_del.id_name} is unknown":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        elif str(e) == "Status not 'draft'. Can't delete.":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=traceback.format_exc(),
            )


app.include_router(router, prefix="/pids", tags=["minter"])
