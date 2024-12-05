#!/bin/bash

set -euo pipefail

file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

file_env "MONGO_PASSWORD"
file_env "DAGSTER_POSTGRES_PASSWORD"

# Note: We specify a directory to Poetry so it knows where to find our `pyproject.toml` file.
#       Reference: https://python-poetry.org/docs/cli/#global-options
#
# TODO: The `dagit` CLI command is deprecated. Use `dagster-webserver` instead.
#
exec poetry run --directory /opt/dagster/lib \
  dagit -h 0.0.0.0 -p 3000 -w workspace.yaml
