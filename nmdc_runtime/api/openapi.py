r"""
This module contains the definitions of constants and functions related to
generating the API's OpenAPI schema (a.k.a. Swagger schema).

References:
- FastAPI Documentation: https://fastapi.tiangolo.com/tutorial/metadata/

Notes:
- The tag descriptions in this file were cut/pasted from `nmdc_runtime/api/main.py`.
  Now that they are in a separate module, we will be able to edit them more easily.
"""

from typing import List, Dict
from enum import Enum


class OpenAPITag(str, Enum):
    r"""A tag you can use to group related API endpoints together in an OpenAPI schema."""

    MINTER = "Persistent identifiers"
    SYSTEM_ADMINISTRATION = "System administration"
    WORKFLOWS = "Workflow management"
    METADATA_ACCESS = "Metadata access"
    USERS = "User accounts"


# Mapping from tag names to their (Markdown-formatted) descriptions.
tag_descriptions: Dict[str, str] = {}

tag_descriptions[
    OpenAPITag.METADATA_ACCESS.value
] = r"""
Retrieve and manage metadata.

The metadata access endpoints fall into several subcategories:

- **Find**: Find a few types of metadata, using a simplified syntax.
    - Each endpoint deals with a predetermined type of metadata; i.e., [studies](https://w3id.org/nmdc/Study/), [biosamples](https://w3id.org/nmdc/Biosample/), [data objects](https://w3id.org/nmdc/DataObject/), [planned processes](https://w3id.org/nmdc/PlannedProcess/), or [workflow executions](https://w3id.org/nmdc/WorkflowExecution/).
- **NMDC schema**: Examine the [NMDC schema](https://microbiomedata.github.io/nmdc-schema/), itself, and use schema-related terminology to find metadata of any type.
- **Queries**: Find, update, and delete metadata using [MongoDB commands](https://www.mongodb.com/docs/manual/reference/command/#user-commands).
- **Changesheets**: Modify metadata by uploading [changesheets](https://docs.microbiomedata.org/runtime/howto-guides/author-changesheets/).
- **JSON operations**: Insert or update metadata by submitting a JSON document representing a [Database](https://w3id.org/nmdc/Database/).
"""

tag_descriptions[
    OpenAPITag.WORKFLOWS.value
] = r"""
Manage workflows and their execution.

The workflow management endpoints fall into several subcategories:

- **Sites**: Register compute sites that can execute workflows, and generate credentials for them.
    - A site corresponds to a physical place that may participate in job execution.
    - A site may register data objects and capabilities with the Runtime. It may claim jobs to execute, and it may update job operations with execution info.
    - A site must be able to service requests for any data objects it has registered.
    - A site may expose a "put object" custom method for authorized users. This method facilitates an operation to upload an object to the site and have the site register that object with the Runtime system.
- **Workflows**: Manage workflow templates, which serve as blueprints for job execution.
    - A workflow is a template for creating jobs.
    - Workflow jobs are typically created by the system via triggers, which are associations between workflows and data object types.
- **Capabilities**: Manage the technical requirements that sites must meet to execute specific workflows.
    - A workflow may require a site that executes it to have specific capabilities.
    - These capabilities may go beyond the simple ability to access the data objects registered with the Runtime system.
    - Sites register their capabilities, and sites are only able to claim workflow jobs if those sites have the capabilities required by the workflow.
- **Object types**: Manage the types of data objects whose creation can trigger job creation and, eventually, workflow execution.
    - A data object type is an annotation that can be applied to data objects.
    - A data object may have one or more types. Those types can be associated with workflows, through triggers.
- **Triggers**: Define associations between workflows and object types to enable automatic job creation.
    - A [trigger](https://docs.microbiomedata.org/runtime/howto-guides/create-triggers/) is an association between a workflow and a data object type.
    - When a data object is [annotated with a type](https://docs.microbiomedata.org/runtime/nb/queue_and_trigger_data_jobs/#use-case-annotate-a-known-object-with-a-type-that-will-trigger-a-workflow)—which may occur shortly after object registration—the Runtime will check—via trigger associations—whether it is due to create any jobs.
- **Jobs**: Manage the [claiming](https://docs.microbiomedata.org/runtime/howto-guides/claim-and-run-jobs/) and status of workflow executions.
    - A job is a resource that decouples the configuration of a workflow, from execution of that workflow.
    - Rather than directly creating a workflow operation, the Runtime creates a job that pairs a workflow with its configuration. Then, a site can claim the job—by its ID—and execute the associated workflow without doing additional configuration.
    - A job can have multiple executions. All executions of all jobs of a given workflow, make up that workflow's executions.
    - A site that already has a compatible job execution result can preempt the unnecessary creation of a job by _pre-claiming_ it. This will return like a claim, and now the site can register known data object inputs for the job without the risk of the Runtime creating a claimable job of the pre-claimed type.
- **Objects**: Manage the Data Repository Service (DRS) objects that are inputs and outputs of workflow executions.
    - A [Data Repository Service (DRS) object](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_datatypes) represents content necessary for—or content produced by—job execution.
    - An object may be a *blob* (analogous to a file) or a *bundle* (analogous to a folder). Sites register objects, and sites must ensure that these objects are accessible to the "NMDC data broker."
    - An object may be annotated with one or more object types, useful for triggering workflows.
- **Operations**: Track and monitor the real-time execution status of claimed jobs, including progress updates and error handling.
    - An operation is a resource for tracking the execution of a job.
    - When a job is claimed by a site for execution, an operation resource is created.
    - An operation is like a "promise," in that it should eventually resolve to either a successful result—i.e., an execution resource—or to an error.
    - An operation is parameterized to return a result type, and a metadata type for storing progress information, that are both particular to the job type.
    - Operations may be paused, resumed, and/or cancelled.
    - Operations may expire, i.e. not be stored indefinitely. In this case, it is recommended that execution resources have longer lifetimes/not expire, so that information about successful results of operations are available.
- **Runs**: _(work in progress)_ Execute simple jobs and report execution events back to the Runtime.
    - Run simple jobs.
    - For off-site job runs, keep the Runtime appraised of run events.
"""

tag_descriptions[
    OpenAPITag.USERS.value
] = r"""
Create and manage user accounts.
"""

tag_descriptions[
    OpenAPITag.MINTER.value
] = r"""
Mint and manage persistent identifiers.
"""

tag_descriptions[
    OpenAPITag.SYSTEM_ADMINISTRATION.value
] = r"""
Retrieve information about the software components that make up the Runtime.
"""

# Remove leading and trailing whitespace from each description.
for name, description in tag_descriptions.items():
    tag_descriptions[name] = description.strip()

ordered_tag_descriptors: List[Dict] = [
    {
        "name": OpenAPITag.METADATA_ACCESS.value,
        "description": tag_descriptions[OpenAPITag.METADATA_ACCESS.value],
    },
    {
        "name": OpenAPITag.WORKFLOWS.value,
        "description": tag_descriptions[OpenAPITag.WORKFLOWS.value],
    },
    {
        "name": OpenAPITag.MINTER.value,
        "description": tag_descriptions[OpenAPITag.MINTER.value],
    },
    {
        "name": OpenAPITag.USERS.value,
        "description": tag_descriptions[OpenAPITag.USERS.value],
    },
    {
        "name": OpenAPITag.SYSTEM_ADMINISTRATION.value,
        "description": tag_descriptions[OpenAPITag.SYSTEM_ADMINISTRATION.value],
    },
]


def make_api_description(api_version: str, schema_version: str) -> str:
    r"""
    Returns an API description into which the specified schema version string has been incorporated.

    Args:
        api_version (str): The version of this Runtime instance.
        schema_version (str): The version of `nmdc-schema` the Runtime is using.

    Returns:
        str: The Markdown-formatted API description.
    """
    result = f"""
Welcome to the **NMDC Runtime API**, an API you can use to [access metadata](https://docs.microbiomedata.org/howto_guides/api_gui/) residing in the NMDC database.

Users having adequate permissions can also use it to generate identifiers, submit metadata,
and manage workflow executions.

##### Quick start

The endpoints of the NMDC Runtime API are listed below.
They are organized into sections, each of which can be opened and closed.
The endpoints, themselves, can also be opened and closed.

Each endpoint—when opened—has a "Try it out" button, which you can press in order to send a request
to the endpoint directly from this web page. Each endpoint can also be
[accessed programmatically](https://docs.microbiomedata.org/runtime/nb/api_access_via_python/).

Some endpoints have a padlock icon, which means that the endpoint is only accessible to logged-in users.
You can log in by clicking the "Authorize" button located directly above the list of endpoints.

##### Contact us

You can [contact us](https://microbiomedata.org/contact/) anytime.
We continuously refine the API and may be able to streamline your use case.

##### Versions

[NMDC Runtime](https://docs.microbiomedata.org/runtime/) version: `{api_version}`

[NMDC Schema](https://microbiomedata.github.io/nmdc-schema/) version: `{schema_version}`
""".strip()
    return result
