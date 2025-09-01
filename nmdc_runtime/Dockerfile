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

# Install Python dependencies (both development and production dependencies).
# Note: We copy only the files that `uv` needs in order to install dependencies.
#       This allows us to leverage Docker's layer caching in more situations.
COPY ./pyproject.toml /code/pyproject.toml
COPY ./uv.lock /code/uv.lock
COPY ./nmdc_runtime/__init__.py /code/nmdc_runtime/__init__.py
RUN cd /code && uv sync

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS fastapi
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
COPY . /code

# Install the project in editable mode, and uninstall development dependencies.
RUN cd /code && uv sync --no-dev

# Use Uvicorn to serve the FastAPI app on port 8000.
EXPOSE 8000
WORKDIR /code
CMD ["uv", "run", "uvicorn", "nmdc_runtime.api.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS dagster
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
#
# Note: This path (i.e. "/opt/dagster/lib/") is hard-coded in a few places in `nmdc_runtime/site/ops.py`. That's why
#       this image does not store the repository contents in `/code`, unlike the other images in this Dockerfile.
#
COPY . /opt/dagster/lib

# Install the project in editable mode, and uninstall development dependencies.
RUN cd /opt/dagster/lib && uv sync --no-dev

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

# Install the project in editable mode.
RUN cd /code && uv sync

# Make `wait-for-it.sh` executable.
RUN chmod +x /code/.docker/wait-for-it.sh

WORKDIR /code

# Ensure started container does not exit, so that a subsequent `docker exec` command can run tests.
# For an example `docker exec` command, see `Makefile`'s `run-test` target.
# Such a command should use `wait-for-it.sh` to run `pytest` no earlier than when the FastAPI server is accessible.
ENTRYPOINT ["tail", "-f", "/dev/null"]