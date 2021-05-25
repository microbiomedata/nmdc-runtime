from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from nmdc_runtime.api.endpoints import (
    users,
    operations,
    sites,
    jobs,
    objects,
    capabilities,
    triggers,
)

api_router = APIRouter()
api_router.include_router(users.router, tags=["users"])
api_router.include_router(operations.router, tags=["operations"])
api_router.include_router(sites.router, tags=["sites"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(objects.router, tags=["objects"])
api_router.include_router(capabilities.router, tags=["capabilities"])
api_router.include_router(triggers.router, tags=["triggers"])

tags_metadata = [
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
        "name": "sites",
        "description": (
            """A site corresponds to a physical place that may participate in job execution.

A site may register data objects and capabilties with NMDC. It may claim jobs to execute, and it may update job operations with execution info.

A site must be able to service requests for any data objects it has registered."""
        ),
    },
    {
        "name": "operations",
        "description": """An operation is a resource for tracking the execution of a job.

When a job is claimed by a site for execution, an operation resource is created.

An operation is akin to a "promise" or "future" in that it should eventually resolve to either a successful result, i.e. an execution resource, or to an error.

An operation is parameterized to return a result type, and a metadata type for storing progress information, that are both particular to the job type.

Operations may be paused, resumed, and/or cancelled.

Operations may expire, i.e. not be stored indefinitely. In this case, it is recommended that execution resources have longer lifetimes / not expire, so that information about successful results of operations are available.
        """,
    },
    {
        "name": "jobs",
        "description": """A job is a resource that isolates workflow configuration from execution.

Rather than directly creating a workflow operation by supplying a workflow ID along with configuration, NMDC creates a job that pairs a workflow with configuration. Then, a site can claim a job ID, allowing the site to execute the intended workflow without additional configuration.

A job can have multiple executions, and a workflow's executions are precisely the executions of all jobs created for that workflow.
        """,
    },
    {
        "name": "objects",
        "description": (
            "A [Data Repository Service (DRS) object](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_datatypes) represents content necessary"
            " for a workflow job to execute, and/or output from a job execution."
            " An object may be a *blob*, analogous to a file, or a *bundle*, analogous to a folder."
            " Sites register objects, and sites must ensure "
            " that these objects are accessible to the NMDC data broker."
        ),
    },
    {
        "name": "capabilities",
        "description": (
            "A workflow may need an executing site to have certain capabilities"
            " beyond the simple fetching of identified data objects."
            " Sites register capabilties, and sites are only able to accept"
            " workflow job operations if they are known to have the capabiltiies needed"
            " for the job."
        ),
    },
]

app = FastAPI(
    title="NMDC Runtime API",
    version="0.1.0",
    description=(
        "This is a draft of the NMDC Runtime API."
        " The resource layout currently covers aspects of workflow execution and automation,"
        " and is intended to facilitate discussion as more of the API is developed."
    ),
    openapi_tags=tags_metadata,
)
app.include_router(api_router)


origins = [
    "http://localhost:8001",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# To build static redoc documentation:
# > cd docs/design && redoc-cli bundle http://0.0.0.0:8000/openapi.json

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
