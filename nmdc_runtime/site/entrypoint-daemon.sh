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

# Note: The `--no-sync` flag has no effect when used outside of a project,
#       so we omit it from this command. If we were to include it here, uv
#       would display a warning saying exactly that.
exec uv run --active dagster-daemon run
