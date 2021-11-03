# Best practice: Choose a stable base image and tag.
FROM tiangolo/uvicorn-gunicorn:python3.8-slim

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
# WORKDIR is /app/ FROM tiangolo/uvicorn-gunicorn:python3.8-slim
COPY requirements/main.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Add repository code
COPY . .
RUN pip install --no-cache-dir --editable .[dev]

# Best practices: Prepare for C crashes.
ENV PYTHONFAULTHANDLER=1

# For development: CMD ["/start-reload.sh"]
ENTRYPOINT ["pytest", "-x"]