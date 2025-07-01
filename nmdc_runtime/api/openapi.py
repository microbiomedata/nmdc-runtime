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

# Mapping from tag names to their (Markdown-formatted) descriptions.
tag_descriptions: Dict[str, str] = {}

tag_descriptions[
    "sites"
] = r"""
A site corresponds to a physical place that may participate in job execution.

A site may register data objects and capabilities with NMDC. It may claim jobs to execute, and it may
update job operations with execution info.

A site must be able to service requests for any data objects it has registered.

A site may expose a "put object" custom method for authorized users. This method facilitates an
operation to upload an object to the site and have the site register that object with the runtime
system.
"""

tag_descriptions[
    "workflows"
] = r"""
A workflow is a template for creating jobs.

Workflow jobs are typically created by the system via trigger associations between
workflows and object types. A workflow may also require certain capabilities of sites
in order for those sites to claim workflow jobs.
"""

tag_descriptions[
    "users"
] = r"""
Endpoints for user identification.

Currently, accounts for use with the Runtime API are created manually by system administrators.
"""

tag_descriptions[
    "capabilities"
] = r"""
A workflow may require an executing site to have particular capabilities.

These capabilities go beyond the simple ability to access the data object resources registered with
the runtime system. Sites register their capabilities, and sites are only able to claim workflow
jobs if they are known to have the capabilities required by the workflow.
"""

tag_descriptions[
    "object types"
] = r"""
An object type is an object annotation that is useful for triggering workflows.

A data object may be annotated with one or more types, which in turn can be associated with
workflows through trigger resources.

The data-object type system may be used to trigger workflow jobs on a subset of data objects when a
new version of a workflow is deployed. This could be done by minting a special object type for the
occasion, annotating the subset of data objects with that type, and registering the association of
object type to workflow via a trigger resource.
"""

tag_descriptions[
    "triggers"
] = r"""
A trigger is an association between a workflow and a data object type.

When a data object is annotated with a type, perhaps shortly after object registration, the NMDC
Runtime will check, via trigger associations, for potential new jobs to create for any workflows.
"""

tag_descriptions[
    "jobs"
] = r"""
A job is a resource that isolates workflow configuration from execution.

Rather than directly creating a workflow operation by supplying a workflow ID along with
configuration, NMDC creates a job that pairs a workflow with configuration. Then, a site can claim a
job ID, allowing the site to execute the intended workflow without additional configuration.

A job can have multiple executions, and a workflow's executions are precisely the executions of all
jobs created for that workflow.

A site that already has a compatible job execution result can preempt the unnecessary creation of a
job by pre-claiming it. This will return like a claim, and now the site can register known data
object inputs for the job without the risk of the runtime system creating a claimable job of the
pre-claimed type.
"""

tag_descriptions[
    "objects"
] = r"""
A [Data Repository Service (DRS)
object](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_datatypes)
represents content necessary for a workflow job to execute, and/or output from a job execution.

An object may be a *blob*, analogous to a file, or a *bundle*, analogous to a folder. Sites register
objects, and sites must ensure that these objects are accessible to the NMDC data broker.

An object may be associated with one or more object types, useful for triggering workflows.
"""

tag_descriptions[
    "operations"
] = r"""
An operation is a resource for tracking the execution of a job.

When a job is claimed by a site for execution, an operation resource is created.

An operation is akin to a "promise" or "future" in that it should eventually resolve to either a
successful result, i.e. an execution resource, or to an error.

An operation is parameterized to return a result type, and a metadata type for storing progress
information, that are both particular to the job type.

Operations may be paused, resumed, and/or cancelled.

Operations may expire, i.e. not be stored indefinitely. In this case, it is recommended that
execution resources have longer lifetimes / not expire, so that information about successful results
of operations are available.
"""

tag_descriptions[
    "queries"
] = r"""
A query is an operation (find, update, etc.) against the metadata store.

Metadata -- for studies, biosamples, omics processing, etc. -- is used by sites to execute jobs,
as the parameterization of job executions may depend not only on the content of data objects, but
also on objects' associated metadata.

Also, the function of many workflows is to extract or produce new metadata. Such metadata products
should be registered as data objects, and they may also be supplied by sites to the runtime system
as an update query (if the latter is not done, the runtime system will sense the new metadata and
issue an update query).
"""

tag_descriptions[
    "metadata"
] = r"""
The [metadata endpoints](https://api.microbiomedata.org/docs#/metadata) can be used to get and filter
metadata from collection set types (including 
[studies](https://w3id.org/nmdc/Study/), 
[biosamples](https://w3id.org/nmdc/Biosample/), 
[planned processes](https://w3id.org/nmdc/PlannedProcess/), and 
[data objects](https://w3id.org/nmdc/DataObject/) 
as discussed in the __find__ section).
<br/>
 
The __metadata__ endpoints allow users to retrieve metadata from the data portal using the various
GET endpoints  that are slightly different than the __find__ endpoints, but some can be used similarly.
As with the __find__ endpoints,  parameters for the __metadata__ endpoints that do not have a
red ___* required___ next to them are optional. <br/>

Unlike the compact syntax used in the __find__  endpoints, the syntax for the filter parameter of
the metadata endpoints
uses [MongoDB-like language querying](https://www.mongodb.com/docs/manual/tutorial/query-documents/).
"""

tag_descriptions[
    "find"
] = r"""
The [find endpoints](https://api.microbiomedata.org/docs#/find) are provided with NMDC metadata entities
already specified - where metadata about [studies](https://w3id.org/nmdc/Study),
[biosamples](https://w3id.org/nmdc/Biosample), [data objects](https://w3id.org/nmdc/DataObject/),
and [planned processes](https://w3id.org/nmdc/PlannedProcess/) can be retrieved using GET requests.
<br/>

Each endpoint is unique and requires the applicable attribute names to be known in order to structure a query
in a meaningful way.  Parameters that do not have a red ___* required___ label next to them are optional.
"""

tag_descriptions[
    "runs"
] = r"""
**WORK IN PROGRESS**

Run simple jobs.

For off-site job runs, keep the Runtime appraised of run events.
"""

# Remove leading and trailing whitespace from each description.
for name, description in tag_descriptions.items():
    tag_descriptions[name] = description.strip()

ordered_tag_descriptors: List[Dict[str, str]] = [
    {"name": "sites", "description": tag_descriptions["sites"]},
    {"name": "users", "description": tag_descriptions["users"]},
    {"name": "workflows", "description": tag_descriptions["workflows"]},
    {"name": "capabilities", "description": tag_descriptions["capabilities"]},
    {"name": "object types", "description": tag_descriptions["object types"]},
    {"name": "triggers", "description": tag_descriptions["triggers"]},
    {"name": "jobs", "description": tag_descriptions["jobs"]},
    {"name": "objects", "description": tag_descriptions["objects"]},
    {"name": "operations", "description": tag_descriptions["operations"]},
    {"name": "queries", "description": tag_descriptions["queries"]},
    {"name": "metadata", "description": tag_descriptions["metadata"]},
    {"name": "find", "description": tag_descriptions["find"]},
    {"name": "runs", "description": tag_descriptions["runs"]},
]
