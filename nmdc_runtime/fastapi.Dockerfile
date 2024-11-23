# Best practice: Choose a stable base image and tag.
FROM python:3.10-slim-bullseye

# Install security updates, and some useful packages.
#
# Best practices:
# * Make sure apt-get doesn't run in interactive mode.
# * Update system packages.
# * Pre-install some useful tools.
# * Minimize system package installation.
RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y --no-install-recommends tini procps net-tools \
  build-essential git make zip && \
  apt-get -y clean && \
  rm -rf /var/lib/apt/lists/*

# Install Poetry.
RUN pip install poetry

WORKDIR /code

# Use Poetry to install production Python dependencies.
COPY ./pyproject.toml /code/pyproject.toml
COPY ./poetry.lock /code/poetry.lock
RUN poetry install --without dev

# Add repository code
COPY . /code

CMD ["python", "-m", "poetry", "run", "uvicorn", "nmdc_runtime.api.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]