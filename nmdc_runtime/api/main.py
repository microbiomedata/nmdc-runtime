import os
from contextlib import asynccontextmanager
from importlib import import_module

import pkg_resources
import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from nmdc_runtime.util import all_docs_have_unique_id, ensure_unique_id_indexes
from nmdc_runtime.api.core.auth import get_password_hash
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
        "description": "Tools for metadata validation and registration/submission.",
    },
    {
        "name": "find",
        "description": "Find NMDC metadata entities.",
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
            doc = model.dict()
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
            ).dict(exclude_unset=True),
            upsert=True,
        )
        mdb.users.create_index("username")

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
            ).dict(),
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
            "dwinston",
            "mam",
            "montana",
            "pajau",
            "spatil",
        ],
        "/queries:run(query_cmd:DeleteCommand)": [
            "admin",
            "dwinston",
            "scanon",
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


app = FastAPI(
    title="NMDC Runtime API",
    version="0.1.0",
    description=(
        "This is a draft of the NMDC Runtime API."
        " The resource layout currently covers aspects of workflow execution and automation,"
        " and is intended to facilitate discussion as more of the API is developed."
        "\n\n"
        "Dependency versions:\n\n"
        f'nmdc-schema={pkg_resources.get_distribution("nmdc_schema").version}'
    ),
    openapi_tags=tags_metadata,
    lifespan=lifespan,
)
app.include_router(api_router)


app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"(http://localhost:\d+)|(https://.+?\.microbiomedata\.org)",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
