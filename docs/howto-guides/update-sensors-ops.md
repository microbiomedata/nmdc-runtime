# Update Sensors, Jobs, Graphs, and Ops

This guide will walk through the operation of the underlying [Dagster](https://docs.dagster.io)
orchestrator used by the Runtime, using a particular
[Sensor](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) as the entry point.

A Dagster [Repository](https://docs.dagster.io/concepts/repositories-workspaces/repositories) is a
collection of code that defines how orchestration is to be done. The
[`nmdc_runtime.site.repository`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/repository.py)
module exposes three such repositories via the `@repository` decorator. The creatively named `repo`
repository is the main one. The `translation` and `test_translation` repositories are used for GOLD
database translation jobs.

Why multiple repositories? A given repository may require resources that a given Dagster deployment
may not provide -- it is nice to opt-in to serve a given repository of functionality.

A Dagster [Workspace](https://docs.dagster.io/concepts/repositories-workspaces/workspaces) loads one
or more repositories for a given deployment.

A Dagster [Sensor](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) defines a
process that will be run over and over again at a regular, relatively tight interval such as every
30 seconds. The `claim_and_run_apply_changesheet_jobs` sensor, defined in
[`nmdc_runtime.site.repository`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/repository.py)
via the `@sensor` decorator, is the example we'll explore here.

A sensor decides, via the code in its body, to yield one or more `RunRequest`s, requests to run a
particular job with a particular configuration. A sensor may also yield a `SkipReason` to log why no
job requests were yielded for a particular run of the sensor.

What is a Dagster "job"? A Dagster
[Job](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs) is an executable (directed
acyclic) graph of operations. Breaking this down, a Dagster
[Graph](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs) will define abstract
dependencies such as "I need a resource called 'mongo' for my operations". A Dagster
[Op](https://docs.dagster.io/concepts/ops-jobs-graphs/ops), or Operation, is a Python function that
performs work and that depends on resources, e.g. "mongo", that are made accessible to it at
runtime. Finally, a Job is a configuration of a Graph that makes resources more concrete, e.g. "by
'mongo', I mean instantiate this Python class, passing it these parameters with values fetched from
these environment variables."

In the case of the `claim_and_run_apply_changesheet_jobs` sensor, the kind of job that it yields
`RunRequest`s for is given as an argument to the `@sensor` decorator, i.e.

```python
@sensor(job=apply_changesheet.to_job(**preset_normal))
def claim_and_run_apply_changesheet_jobs(_context):
    ...
```

where `apply_changesheet` is a Graph definition and `preset_normal` is a Python dictionary that
supplies resource definitions (a mapping of resource names to Python functions decorated with
`@resource`) as well as configuration for the resources (incl. specifying environment variables to
source).

The `apply_changesheet` Graph is defined in [`nmdc_runtime.site.graphs`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/graphs.py)
 as follows:

```python
@graph
def apply_changesheet():
    sheet_in = get_changesheet_in()
    perform_changesheet_updates(sheet_in)
```

which is rendered in Dagster's [Dagit UI](https://docs.dagster.io/concepts/dagit/dagit) as a graph:

<figure markdown style="max-width: 25em">
  ![Dagit UI rendering of `apply_changesheet` job](../img/dagit-apply-changesheet-job.png)
  <figcaption>Dagit UI rendering of `apply_changesheet` job</figcaption>
</figure>

Thus, Dagster inspects the Python code's abstract syntax tree (AST) in order to create a graph of
operation nodes and their input/output dependencies. You can explore the Dagit rendering of this job
at `/workspace/repo@nmdc_runtime.site.repository:repo/jobs/apply_changesheet/` on your local
instance (at `http://localhost:3000` by default) or e.g. [the read-only deployed
version](https://dagit-readonly.nmdc-runtime-dev.polyneme.xyz/workspace/repo@nmdc_runtime.site.repository:repo/jobs/apply_changesheet/).

The operations comprising `apply_changesheet` are `@op`-decorated functions in
[`nmdc_runtime.site.ops`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/ops.py), i.e.

```python
@op(required_resource_keys={"mongo"})
def get_changesheet_in(context) -> ChangesheetIn:
    mdb: MongoDatabase = context.resources.mongo.db
    object_id = context.solid_config.get("object_id")
    ...
```

and 

```python
@op(required_resource_keys={"mongo"})
def perform_changesheet_updates(context, sheet_in: ChangesheetIn):
    ....
```

Here you can see that Dagster operations (Ops) communicate what resources the need when they are
run. [Resources](https://docs.dagster.io/concepts/resources) are injected as part of the `context`
argument supplied to the function call. You can also see that other particular values that configure
the job run, and thus are usable as input, can be passed to the specific *solid* being run (*solid*
is Dagster's [deprecated](https://docs.dagster.io/_apidocs/solids#legacy-solids) term for *op*, and
`context` objects now have equivalent `op_config` attributes).

This extra level of indirection allows different schemes for the execution of job steps, i.e. ops.
For example, the default Dagster [Executor](https://docs.dagster.io/deployment/executors) executes
each step in its own spawned system process on the host machine, i.e. multiprocessing. One can also
decide to e.g. execute each step within its own Docker container or even within an ephemeral
kubernetes pod. There is similar system flexibility at the [Run
Launcher](https://docs.dagster.io/deployment/run-launcher) level for each Job (configured graph of
Ops) run, where one can launch jobs runs in new processes (the default), new Docker containers, etc.

You can read more about options for Dagster [Deployment](https://docs.dagster.io/deployment) and see
how the NMDC Runtime deployment is configured via its
[dagster.yaml](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/site/dagster.yaml)
file.

In summary, there are many "touch points" for a given job, such as how it is scheduled (e.g. via a
Sensor or via a [Schedule](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules)
that is more akin to [Cron](https://en.wikipedia.org/wiki/Cron)), how it is configured to source
particular Resources from a Graph template, and how the work of the Graph is split into a collection
of Ops that declare their dependencies so that everything can be run in a suitable order. All of
these touch points are code-based, and so any modifications made to a given job should be expressed
as a git branch for a Pull Request.