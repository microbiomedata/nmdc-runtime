from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from nmdc_runtime.api.endpoints import (
    operations,
    sites,
    jobs,
    data_resources,
    compute_resources,
)

api_router = APIRouter()
api_router.include_router(operations.router, tags=["operations"])
api_router.include_router(sites.router, tags=["sites"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(data_resources.router, tags=["data_resources"])
api_router.include_router(compute_resources.router, tags=["compute_resources"])

tags_metadata = [
    {
        "name": "operations",
        "description": """An operation is a resource for tracking a request to run a job.

When a request to run a job is issued, an operation resource is created.

An operation is akin to a "promise" or "future" in that it should eventually resolve to either a successful result, i.e. an execution resource, or to an error.

An operation is parameterized to return a result type, and a metadata type for storing progress information, that are both particular to the workflow type.

Operations may be paused, resumed, and/or cancelled.

Operations may expire, i.e. not be stored indefinitely. In this case, it is recommended that execution resources have longer lifetimes / not expire, so that information about successful results of operations are available.
        """,
    },
    {
        "name": "sites",
        "description": (
            """A site corresponds to a physical place that may participate in workflow execution.

A site may register compute resources and data resources with NMDC. It may also execute workflows, and may request that workflows be executed.

A site must be able to service requests for any data resources it has registered."""
        ),
    },
    {
        "name": "jobs",
        "description": """A job is a resource that isolates workflow configuration from execution.

Rather than directly requesting a workflow execution by supplying a workflow ID along with configuration, one creates a job that pairs a workflow with configuration. Then, a workflow is executed by supplying a job ID without additional configuration.

A job can have multiple executions, and a workflow's executions are precisely the executions of all jobs created for that workflow.
        """,
    },
    {
        "name": "data_resources",
        "description": (
            "A data resource represents a file or set of files necessary"
            " for a workflow job to execute, and/or output from a job execution."
            " Sites register the data resources they contain, and sites must ensure "
            " that these resources are accessible to the NMDC data broker."
        ),
    },
    {
        "name": "compute_resources",
        "description": (
            "A compute resource is a capability necessary for a workflow to execute."
            "Sites register compute resources, and sites are only able to accept "
            "workflow job operations if they are known to have the compute resources needed "
            "for the job."
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
