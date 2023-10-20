# Claim and Run Jobs

The Runtime advertises jobs to be done, where a [Job](https://api.microbiomedata.org/docs#/jobs)
is a [Workflow](https://api.microbiomedata.org/docs#/workflows) paired with a chosen input
[Object](https://api.microbiomedata.org/docs#/objects). See [Guide - Create Workflow Triggers To
Spawn Jobs](guide-create-triggers.md) to learn how to arrange for jobs of interest to be
automatically available when relevant new workflow inputs are available.

You can list open jobs via [`GET /jobs`](https://api.microbiomedata.org/docs#/jobs/list_jobs_jobs_get). To
claim a job, [`POST` to
`/jobs/{job_id}:claim`](https://api.microbiomedata.org/docs#/jobs/claim_job_jobs__job_id__claim_post). The
response will be an [Operation](https://api.microbiomedata.org/docs#/operations) to track job
execution and send updates to the system.

[`PATCH
/operations/{op_id}`](https://api.microbiomedata.org/docs#/operations/update_operation_operations__op_id__patch)
is how to report state transitions for the job execution. If a job execution has been completed
successfully, has failed, or has been aborted, the `done` field for the operation must be set to
`true`.

Example [sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) that claim
and run jobs are available in the codebase in
[`nmdc_runtime.site.repository`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/repository.py):

* `claim_and_run_metadata_in_jobs` senses new jobs to ingest NMDC-Schema-compliant metadata. Users
  submit metadata as Objects and associate them with the `metadata-in` Object Type. This in turn
  triggers the creation of Jobs (for the `metadata-in-1.0.0` Workflow) to validate and ingest the
  metadata. Because these jobs are not too intensive wrt data or compute, the Runtime itself hosts a
  [Site](https://api.microbiomedata.org/docs#/sites) that claims and runs these jobs. *

* `claim_and_run_apply_changesheet_jobs` senses new jobs to apply changesheets, that is, to apply
  surgical updates to existing metadata. Users submit changesheets via [`POST
  /metadata/changesheets:submit`](https://api.microbiomedata.org/docs#/metadata/submit_changesheet_metadata_changesheets_submit_post),
  which creates an Object on behalf of the user, annotates it with the `metadata-changesheet` Object
  Type. A registered trigger then creates a corresponding `apply-changesheet-1.0.0` job, which this
  sensor senses, allowing the Runtime Site to claim and run the job.

* `claim_and_run_gold_translation_curation` is another example. The jobs that this sensor claims and
  runs are created not by a conventionally registered Trigger, but instead by another sensor,
  `ensure_gold_translation_job`. This pattern may be appropriate if the logic to trigger job creation
  is more nuanced and would benefit from being expressed using Python code rather than as a simple
  data structure as in
  [`nmdc_runtime.api.boot.triggers`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/api/boot/triggers.py).

If your workflow is neither data- nor resource-intensive, you may opt to implement it as a Dagster
[Graph](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs) of [Ops
(Operations)](https://docs.dagster.io/concepts/ops-jobs-graphs/ops) within a Runtime Site
[Repository](https://docs.dagster.io/concepts/repositories-workspaces/repositories), e.g. to include
it in the
[`nmdc_runtime.site.repository`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/repository.py)
module with the above examples. Otherwise, NMDC workflow jobs are generally run at Sites that have
access to more expensive and specialized resources, with job execution state and outputs
communicated and delivered to the Runtime via the API.