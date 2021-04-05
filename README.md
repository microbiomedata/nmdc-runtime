A runtime system for NMDC data management and orchestration.

## Overview

The runtime features:

1. [Dagster](https://docs.dagster.io/concepts) orchestration:
    - `solids` represent individual units of computation
    - `pipelines` are built up from solids
    - `schedules` trigger recurring pipeline runs based on time
    - `sensors` trigger pipeline runs based on external state

2. A [TerminusDB](https://terminusdb.com/) database supporting revision control of schema-validated
data.
   
3. A [FastAPI](https://fastapi.tiangolo.com/) service to interface with the orchestrator and
database.

Welcome to your new Dagster repository.

## Local Development

1. Start the [Dagit process](https://docs.dagster.io/overview/dagit). This will start a Dagit web
server that, by default, is served on http://localhost:3000.

```bash
dagit -w nmdc_runtime/dagster_workspace.yaml
```

2. (Optional) If you want to enable Dagster
[Schedules](https://docs.dagster.io/overview/schedules-sensors/schedules) or
[Sensors](https://docs.dagster.io/overview/schedules-sensors/sensors) for your pipelines, start the
[Dagster Daemon process](https://docs.dagster.io/overview/daemon#main) **in a different shell or terminal**:

```bash
dagster-daemon run
```

## Local Testing

Tests can be found in `tests` and are run with the following command:

```bash
pytest tests
```

As you create Dagster solids and pipelines, add tests in `tests/` to check that your
code behaves as desired and does not break over time.

[For hints on how to write tests for solids and pipelines in Dagster,
[see our documentation tutorial on Testing](https://docs.dagster.io/tutorial/testable).
