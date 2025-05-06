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

# Install Python dependencies into the image.
#
# Note: Because these `pip install` commands would consistently time out for one of our developers while he would build
#       this container image, we have overridden pip's default socket timeout (from 15 → 30 seconds) and retry count
#       (from 5 → 10 times). With those overrides in place, these `pip install` commands run OK for that developer.
#
#       References:
#       - https://stackoverflow.com/a/59796952 (source of the workaround)
#       - https://pip.pypa.io/en/stable/cli/pip/#cmdoption-timeout (pip's CLI options)
#
COPY ./requirements/main.txt /code/requirements/main.txt
RUN pip install \
    --timeout 30 \
    --retries 10 \
    --no-cache-dir \
    -r /code/requirements/main.txt

# ────────────────────────────────────────────────────────────────────────────┐
FROM base AS fastapi
# ────────────────────────────────────────────────────────────────────────────┘

# Copy repository contents into image.
COPY . /code

# Install the project in editable mode.
RUN cd /code && \
    pip install --no-cache-dir --editable .

# Use Uvicorn to serve the FastAPI app on port 8000.
EXPOSE 8000
WORKDIR /code
CMD ["uvicorn", "nmdc_runtime.api.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]

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
RUN cd /opt/dagster/lib && \
    pip install --no-cache-dir --editable .

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

# Copy dependency lists into image.
COPY ./requirements /code/requirements

# Install development-only Python dependencies into the image.
RUN pip install \
    --timeout 30 \
    --retries 10 \
    --no-cache-dir \
    -r /code/requirements/dev.txt

# Copy all repository contents into image.
COPY . /code

# Install the project in editable mode.
RUN cd /code && \
    pip install --no-cache-dir --editable .

# Make `wait-for-it.sh` executable.
RUN chmod +x /code/.docker/wait-for-it.sh

WORKDIR /code

# Ensure started container does not exit, so that a subsequent `docker exec` command can run tests.
# For an example `docker exec` command, see `Makefile`'s `test-exec` target.
# Such a command should use `wait-for-it.sh` to run `pytest` no earlier than when the FastAPI server is accessible.
ENTRYPOINT ["tail", "-f", "/dev/null"]