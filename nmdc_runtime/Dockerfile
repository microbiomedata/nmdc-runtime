# Note: Most of the steps for the `base` image were copied verbatim from either `fastapi.Dockerfile`,
#       `dagster.Dockerfile`, or `test.Dockerfile` (indeed, most of the steps were present in all three files).
#       Reference: https://docs.docker.com/get-started/docker-concepts/building-images/multi-stage-builds/
#
# Base this image upon a variant of the official Python 3.10 image that is, in turn,
# based upon a minimal (slim) variant of the Debian 13 (trixie) image.
# Reference: https://hub.docker.com/_/python
# ────────────────────────────────────────────────────────────────────────────┐
FROM python:3.10-slim-trixie AS base
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
# Note: We specify the path to the Python virtual environment that we want `uv` to create inside the
#       container. We do this so that, when we later `COPY` the repository file tree (or a user of
#       the image bind mounts it) into the container, it does not collide with the virtual environment.
#       References:
#       - https://docs.astral.sh/uv/pip/environments/#using-arbitrary-python-environments (RE: `VIRTUAL_ENV`)
#       - https://docs.astral.sh/uv/concepts/projects/sync/#partial-installations (RE: `--no-install-project`)
#
# Note: In the `RUN` command, we use a "cache mount" (a feature of Docker) to cache production dependencies
#       across builds. This is a performance optimization technique shown in the `uv` docs.
#       Reference:
#       - https://docs.astral.sh/uv/guides/integration/docker/#caching (RE: the technique)
#       - https://docs.docker.com/build/cache/optimize/#use-cache-mounts (RE: the feature)
#       - https://docs.astral.sh/uv/reference/settings/#link-mode (RE: `UV_LINK_MODE`)
#
WORKDIR /code
ENV VIRTUAL_ENV="/venv"
RUN mkdir -p "${VIRTUAL_ENV}"
COPY ./pyproject.toml ./pyproject.toml
COPY ./uv.lock        ./uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    UV_LINK_MODE=copy \
    uv sync --active --no-editable --no-dev --no-install-project

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS fastapi
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
WORKDIR /code
COPY . .

# Install the project in editable mode, and compile Python source code into bytecode.
# Note: Compiling into bytecode can speed up app startup time.
#       Reference: https://docs.astral.sh/uv/guides/integration/docker/#compiling-bytecode
RUN uv sync --active --no-dev --compile-bytecode

# Use Uvicorn to serve the FastAPI app on port 8000.
EXPOSE 8000
CMD ["uv", "run", "--active", "python", "-m", "uvicorn", "nmdc_runtime.api.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS dagster
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
#
# Note: This path (i.e. "/opt/dagster/lib/") is hard-coded in a few places in `nmdc_runtime/site/ops.py`. That's why
#       this image does not store the repository contents in `/code`, unlike the other images in this Dockerfile.
#
WORKDIR /opt/dagster/lib
COPY . .

# Install the project in editable mode.
RUN uv sync --active --no-dev

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
WORKDIR /code
COPY . .

# Install the project in editable mode, and install development dependencies (e.g. `pytest`).
RUN uv sync --active

# Make `wait-for-it.sh` executable.
RUN chmod +x ./.docker/wait-for-it.sh

# Ensure started container does not exit, so that a subsequent `docker exec` command can run tests.
# For an example `docker exec` command, see `Makefile`'s `run-test` target.
# Such a command should use `wait-for-it.sh` to run `pytest` no earlier than when the FastAPI server is accessible.
ENTRYPOINT ["tail", "-f", "/dev/null"]
