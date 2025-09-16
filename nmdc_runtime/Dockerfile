# Note: Most of the steps for the `base` image were copied verbatim from either `fastapi.Dockerfile`,
#       `dagster.Dockerfile`, or `test.Dockerfile` (indeed, most of the steps were present in all three files).
#       Reference: https://docs.docker.com/get-started/docker-concepts/building-images/multi-stage-builds/
#
# Base this image upon a variant of the official Python 3.10 image that is, in turn,
# based upon a minimal (slim) variant of the Debian 11 (bullseye) image.
# Reference: https://hub.docker.com/_/python
# ────────────────────────────────────────────────────────────────────────────┐
FROM python:3.10-slim-bullseye AS base
# ────────────────────────────────────────────────────────────────────────────┘

# Install and upgrade system-level software in a non-interactive way, then delete temporary files.
# Note: Setting `DEBIAN_FRONTEND=noninteractive` and passing `-y` to `apt-get` makes things non-interactive.
RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y --no-install-recommends \
    tini \
    procps \
    net-tools \
    build-essential \
    git \
    make \
    zip \
    curl \
    wget \
    gnupg && \
  apt-get -y clean && \
  rm -rf /var/lib/apt/lists/*

# Enable Python's "fault handler" feature, so, when low-level errors occur (e.g. segfaults), Python prints lots of info.
# Reference: https://docs.python.org/3/using/cmdline.html#envvar-PYTHONFAULTHANDLER
ENV PYTHONFAULTHANDLER=1

# Configure Git to consider the `/code` directory to be "safe", so that, when a Git repository
# created outside of the container gets mounted at that path within the container, the
# `uv-dynamic-versioning` tool running within the container does not fail with the error:
# > "Detected Git repository, but failed because of dubious ownership"
# Reference: https://git-scm.com/docs/git-config#Documentation/git-config.txt-safedirectory
RUN git config --global --add safe.directory /code

# Install `uv`.
# Reference: https://docs.astral.sh/uv/guides/integration/docker/#installing-uv
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && \
    rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# Install Python dependencies (production dependencies only).
#
# Note: We copy only the files that `uv` needs in order to install dependencies. That way,
#       we minimize the number of files whose changes would invalidate cached image layers
#
# Note: We use the `VIRTUAL_ENV` environment variable to specify the path to the Python virtual
#       environment that we want the `uv` program inside the container to create and use.
#
#       Q: Why don't we use `./.venv` in the repository file tree?
#       A: If we were to do that, then, whenever a developer would mount (via our Docker Compose file)
#          the repository file tree from their host machine (which may include a `.venv/` directory
#          created by their host machine) into the container, it would overwrite the Python virtual
#          environment that the `uv` program inside the container is using.
#
#       Q: What is special about the `VIRTUAL_ENV` environment variable?
#       A: When using `uv`'s `--active` option (as we do in later stages of this Dockerfile),
#          `uv` determines which virtual environment is active by looking at `VIRTUAL_ENV'. This
#          is the case, even though the documentation of the `venv` module (in Python's standard
#          library) specifically says: "`VIRTUAL_ENV` cannot be relied upon to determine whether
#          a virtual environment is being used."
#
#       References:
#       - https://docs.astral.sh/uv/pip/environments/#using-arbitrary-python-environments (RE: `VIRTUAL_ENV`)
#       - https://docs.astral.sh/uv/reference/environment/#virtual_env (RE: `VIRTUAL_ENV`, from uv's perspective)
#       - https://docs.python.org/3/library/venv.html#how-venvs-work (RE: `VIRTUAL_ENV`, from venv's perspective)
#       - https://docs.astral.sh/uv/concepts/projects/sync/#partial-installations (RE: `--no-install-project`)
#
# Note: In the `RUN` command, we use a "cache mount" (a feature of Docker) to cache production dependencies
#       across builds. This is a performance optimization technique shown in the `uv` docs.
#       Reference:
#       - https://docs.astral.sh/uv/guides/integration/docker/#caching (RE: the technique)
#       - https://docs.docker.com/build/cache/optimize/#use-cache-mounts (RE: the feature)
#       - https://docs.astral.sh/uv/reference/settings/#link-mode (RE: `UV_LINK_MODE`)
#       - https://docs.astral.sh/uv/reference/cli/#uv-sync--no-install-project (RE: `--no-install-project`)
#
# Note: We use `--compile-bytecode` so that Python compiles `.py` files to `.pyc` files now,
#       instead of when the container is running. By default, `uv` defers this compilation
#       to "import time," whereas `pip` (by default) performs it at "install time" (like this).
#
ENV VIRTUAL_ENV="/venv"
RUN mkdir -p "${VIRTUAL_ENV}"
COPY ./pyproject.toml /code/pyproject.toml
COPY ./uv.lock        /code/uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    cd /code && \
    UV_LINK_MODE=copy uv sync --active --no-dev --no-install-project --compile-bytecode

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS fastapi
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
COPY . /code

# Install the project in editable mode.
RUN --mount=type=cache,target=/root/.cache/uv \
    cd /code && \
    uv sync --active --no-dev

# Use Uvicorn to serve the FastAPI app on port 8000.
EXPOSE 8000
WORKDIR /code
CMD ["uv", "run", "--active", "uvicorn", "nmdc_runtime.api.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS dagster
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
#
# Note: This path (i.e. "/opt/dagster/lib/") is hard-coded in a few places in `nmdc_runtime/site/ops.py`. That's why
#       this image does not store the repository contents in `/code`, unlike the other images in this Dockerfile.
#
COPY . /opt/dagster/lib

# Install the project in editable mode.
RUN --mount=type=cache,target=/root/.cache/uv \
    cd /opt/dagster/lib && \
    uv sync --active --no-dev

# Move Dagster configuration files to the place Dagster expects.
ENV DAGSTER_HOME="/opt/dagster/dagster_home/"
RUN mkdir -p                                             "${DAGSTER_HOME}" && \
    cp /opt/dagster/lib/nmdc_runtime/site/dagster.yaml   "${DAGSTER_HOME}" && \
    cp /opt/dagster/lib/nmdc_runtime/site/workspace.yaml "${DAGSTER_HOME}"

# Use Tini to run Dagit.
#
# Notes:
# - The port number (i.e. "3000") is hard-coded in `nmdc_runtime/site/entrypoint-dagit.sh`.
# - Dagster daemon (versus Dagit) can be launched by overriding the `ENTRYPOINT` defined here.
#
# Reference: https://github.com/krallin/tini
#
EXPOSE 3000
WORKDIR /opt/dagster/dagster_home/
ENTRYPOINT ["tini", "--", "../lib/nmdc_runtime/site/entrypoint-dagit.sh"]

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS test
# ────────────────────────────────────────────────────────────────────────────┘

# Copy all repository contents into image.
COPY . /code

# Install the project in editable mode, and install development dependencies.
RUN --mount=type=cache,target=/root/.cache/uv \
    cd /code && \
    uv sync --active

# Make `wait-for-it.sh` executable.
RUN chmod +x /code/.docker/wait-for-it.sh

WORKDIR /code

# Ensure started container does not exit, so that a subsequent `docker exec` command can run tests.
# For an example `docker exec` command, see `Makefile`'s `run-test` target.
# Such a command should use `wait-for-it.sh` to run `pytest` no earlier than when the FastAPI server is accessible.
ENTRYPOINT ["tail", "-f", "/dev/null"]