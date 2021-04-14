FROM python:3.8-slim-buster

# Checkout and install dagster libraries needed to run the gRPC server that exposes your repository
# to dagit and dagster-daemon, and to load the DagsterInstance

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker

# Set $DAGSTER_HOME and copy dagster instance there

ENV DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p $DAGSTER_HOME

COPY nmdc_runtime/dagster.yaml $DAGSTER_HOME

# Add repository code

WORKDIR /opt/dagster/lib
COPY . .
RUN pip install --editable .

WORKDIR /opt/dagster/app
COPY nmdc_runtime/dagster_repository.py /opt/dagster/app/repo.py

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repo.py"]
