# Scenarios

## Updating a data object

While a data object ID cannot be changed, its access information -- such as its URL -- can be
changed.

## Retiring a data object

A data object may not be deleted.

## Caching a data object hosted elsewhere

Sites may cache data objects by ID. This is because each revision of a file is considered a
different data object and given a different ID. A job references required data objects by ID.
Thus, a particular job requires particular revisions of files.

## A site communicating recoverable failure to execute a job

## A site communicating permanent failure to execute a job

## A site not communicating its failure to execute a job

## Registering metadata for data objects

## Claiming jobs

## A site registering interest in types of jobs

## Registering a workflow and its dependencies

## Triggering workflow-job creation via logic

A data object may be annotated with types (`/objects/{object_id}/types`) that in turn can be mapped
to what workflows it should trigger. This mapping logic can be registered and managed via the API
(`/object_types/{type_id}/workflows`).

## Updating workflows and data products

The data-object type system may be used to trigger workflow jobs on a subset of data objects when a
new version of a workflow is deployed. This could be done by minting a special object type for the
occasion, annotating the subset of data objects with that type, and registering that type to trigger
the new workflow.

## Triggering workflow-job creation via data objects

## Pre-claiming jobs

A site that already has a compatible job execution result can preempt the unnecessary creation of a
job by POSTing to `/jobs:preclaim`. This will return like `/jobs/1:claim`, and now the site can
register known data object inputs for the job without the risk of the NMDC Runtime creating a
claimable job of the pre-claimed type.

# Comparison to Global Alliance for Genomics and Health (GA4GH) APIs

## GA4GH Workflow Execution Service (WES) API

The GA4GH [Workflow Execution Service (WES)
API](https://ga4gh.github.io/workflow-execution-service-schemas/docs/) has different goals than the
NMDC Runtime API. The GA4GH WES API allows users to directly request that workflows be run, to pass
parameters for the workflow, to get information about running workflows, and to cancel a running
workflow. Of these, the NMDC Runtime API only seeks to support users getting information about
running workflows.

The NMDC Runtime determines what workflows are ready to be run by monitoring data dependencies,
parameterizes the workflows appropriately as new jobs, and allows sites to claim these jobs. Sites
may cancel their claim to execute a job, but they may not cancel a job.

The GA4GH WES API does not address the ability to pass credentials with job claims for access to
data objects. It also does not address the ability for sites to filter for claimable jobs. Finally,
it does not address the ability of each type of job to have a specialized API response schema for
fields such as status, output, etc. The NMDC Runtime API seeks these abilities.

## GA4GH Data Repository Service (DRS) API

The GA4GH [Data Repository Service (DRS)
API](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/)
(formerly called the "Data Object Service" API) is a read-only API, expecting each implementation to
define its own mechanisms and interfaces for adding and updating data.

The NMDC Runtime API may compatibly specialize the GA4GH DRS API in the following ways:

* `updated_time` and `version` in `DrsObject` are ignored because the underlying content of a data
   object cannot be updated.

* optional `_compressionFormat` in `DrsObject` ("zip" or "bz2", otherwise not compressed). The
  leading underscore is in case a later version of the DRS API defines this property.
  
## GA4GH Task Execution Service (TES) API

The GA4GH [Task Execution Service (TES) API](https://ga4gh.github.io/task-execution-schemas/docs/)
has different goals than the NMDC Runtime API. The GA4GH TES API allows users to directly create
tasks (jobs). Furthermore, sites are expected to execute tasks exactly as specified via a specific
sequence of Docker run commands, with given volume mounts, etc.

The NMDC Runtime does not require that executing sites act as TES services that can service TES API
requests. Though the NMDC Runtime could in theory act as a TES server, our needs for task/job schema
and task/job listing are different. We may borrow from the TES API `Task` schema as appropriate for
our `Job` schema.

Finally, we may adopt the [GA4GH service-info
API](https://editor.swagger.io/?url=https://raw.githubusercontent.com/ga4gh-discovery/ga4gh-service-info/develop/service-info.yaml),
convention for the `GetServiceInfo` endpoint that is required by the TES API.

