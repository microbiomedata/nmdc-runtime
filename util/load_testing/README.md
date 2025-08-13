# Load testing

This directory contains files related to using [Locust](https://locust.io/), an open source load testing tool, to perform load testing of the Runtime API.

## Contents

- `locustfile.py`: Definitions of the tasks that will be performed by the "simulated users" spawned by Locust
- `README.md`: This document

## Quick start

All commands shown below were designed to be issued from the root directory of the repository.

1. (Optional) Install the `locust` Python package so your IDE recognizes its
   module/function names.
   ```sh
   # Activate whichever Python virtual environment you use
   # for `nmdc-runtime` development.
   source .venv/bin/activate

   # Install `locust` into that Python virtual environment.
   python -m pip install locust
   ```
   > **Note:** We may eventually designate `locust` as a formal development dependency of `nmdc-runtime` (i.e. we may add it to `requirements/dev.in`). For now, we're opting not to, as we're still experimenting with it.
2. Spin up the standard development stack so we have a Runtime API instance
   to test.
   ```sh
   make up-dev
   ```
3. Spin up Locust [in a Docker container](https://docs.locust.io/en/stable/running-in-docker.html).
   ```sh
   docker run --rm -it \
       --publish 8089:8089 \
       --volume $PWD/util/load_testing:/mnt/locust \
       --network nmdc-runtime-dev_default \
       locustio/locust \
         --locustfile /mnt/locust/locustfile.py \
         --users 1 \
         --spawn-rate 1 \
         --host http://fastapi:8000
   ```
   > **Note:** The `--network nmdc-runtime-dev_default` CLI option causes Docker to connect the Locust container to the same network as the standard development stack. That way, the Locust container will be able to access the Runtime API at `http://fastapi:8000`, regardless of how you access the Runtime API from your host OS.
   >
   > **Note:** The CLI options Locust supports are documented [here](https://docs.locust.io/en/stable/configuration.html#command-line-options). For example, developers sometimes use the `--autostart ` CLI option (so the test starts without requiring us to press the "Start" button on the web UI). Also, developers sometimes use the `--exclude-tags {string}` CLI option (so tasks tagged with `{string}` are skipped). We may eventually specify some CLI options via a [configuration file](https://docs.locust.io/en/stable/configuration.html#configuration-file).
4. Visit the Locust web UI, at: http://localhost:8089
5. Perform load testing.
   - Press the "Start" button to start the test.
   - Explore the "Statistics," "Charts," etc. tabs.
   - Press the "Edit" button to adjust the load.
   - Press the "Stop" button when you're done.
6. (Optional) Stop the Locust container by pressing `^C` (i.e., `⌘` + `C`) in your terminal window.
