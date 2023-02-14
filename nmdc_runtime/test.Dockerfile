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

RUN wget wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
RUN echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list

RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && \
  apt-get install -y mongodb-org && \
  apt-get -y clean && \
  rm -rf /var/lib/apt/lists/*

# Install requirements
WORKDIR /code
COPY ./requirements/main.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Add repository code
COPY . /code
RUN pip install --no-cache-dir --editable .[dev]

# Ensure wait-for-it
RUN chmod +x wait-for-it.sh

# Best practices: Prepare for C crashes.
ENV PYTHONFAULTHANDLER=1

ENTRYPOINT [ "./wait-for-it.sh" , "fastapi:8000" , "--strict" , "--timeout=300" , "--" , "pytest", "-x"]