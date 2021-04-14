FROM python:3.8-slim-buster

# Dagster libraries to run both dagit and the dagster-daemon. Does not need to have access to any
# pipeline code.

RUN pip install \
    dagster \
    dagster-graphql \
    dagit \
    dagster-postgres \
    dagster-docker

# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml $DAGSTER_HOME

COPY dagster_workspace.yaml $DAGSTER_HOME/workspace.yaml

WORKDIR $DAGSTER_HOME
