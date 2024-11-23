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
  build-essential git make zip wget gnupg && \
  apt-get -y clean && \
  rm -rf /var/lib/apt/lists/*

# Install Poetry.
RUN pip install poetry

WORKDIR /code

# Use Poetry to install Python dependencies, including development ones.
COPY ./pyproject.toml /code/pyproject.toml
COPY ./poetry.lock /code/poetry.lock
RUN poetry install --with dev

# Add repository code
COPY . /code

# TODO: Add a claim to the sentence below. Example: "Ensure wait-for-it... is executable"
## Ensure wait-for-it
RUN chmod +x wait-for-it.sh

# Best practices: Prepare for C crashes.
ENV PYTHONFAULTHANDLER=1


# uncomment line below to run all tests
# ENTRYPOINT [ "./wait-for-it.sh", "fastapi:8000" , "--strict" , "--timeout=300" , "--" , "pytest"]

# uncomment line below to stop after first test failure:
# https://docs.pytest.org/en/6.2.x/usage.html#stopping-after-the-first-or-n-failures
ENTRYPOINT [ "./wait-for-it.sh", "fastapi:8000" , "--strict" , "--timeout=300" , "--" , "pytest"]
