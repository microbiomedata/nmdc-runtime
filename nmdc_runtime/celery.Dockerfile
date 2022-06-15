# Best practice: Choose a stable base image and tag.
FROM python:3.9-slim-bullseye

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

WORKDIR /code


COPY ./requirements/main.txt /code/requirements.txt
COPY ./test_data/simple_example.gff /test_data/simple_example.gff
COPY ./test_data/simple_example.gff.md5 /test_data/simple_example.gff.md5

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Add repository code
COPY . /code
RUN pip install --no-cache-dir --editable .

CMD ["celery", "-A", "nmdc_runtime.worker.task_manager", "worker",  "-l", "info", "--events"]
