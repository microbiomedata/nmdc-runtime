#!/bin/sh
# ./entrypoint.sh (to be mounted from host)

# Exit immediately if a command exits with a non-zero status.
set -e

INTERNAL_PORT="8000" # The internal port of your application
ENV_VAR_NAME="MY_DYNAMIC_HOST_PORT" # The environment variable to set

# Attempt to get the full 64-character container ID from cgroup
# This is generally the most reliable method if the cgroup structure is as expected.
CONTAINER_ID=$(grep -o -E '[a-f0-9]{64}' /proc/self/cgroup | head -n 1)

# Fallback to hostname if no 64-char ID was found in cgroup.
# The hostname is often the short container ID and can work for self-inspection.
if [ -z "$CONTAINER_ID" ]; then
    CONTAINER_ID=$(hostname)
    echo "Info: Using hostname ('${CONTAINER_ID}') as Container ID for Docker API lookup."
fi

# Check for Docker socket and a determined Container ID before proceeding
if [ ! -S /var/run/docker.sock ]; then
    echo "Warning: Docker socket /var/run/docker.sock not found. Cannot set ${ENV_VAR_NAME}."
elif [ -z "$CONTAINER_ID" ]; then
    # This case should be rare if hostname fallback works.
    echo "Warning: Could not determine Container ID. Cannot set ${ENV_VAR_NAME}."
else
    # Query Docker API using the determined Container ID
    API_RESPONSE=$(curl --silent --unix-socket /var/run/docker.sock "http://localhost/containers/${CONTAINER_ID}/json")

    # Parse for the host port mapped to the internal port (e.g., 8000/tcp or 8000)
    # The 'jq -r' flag outputs the raw string.
    # The '//' operator in jq provides a fallback if the first path is null.
    HOST_PORT=$(echo "$API_RESPONSE" | jq -r ".NetworkSettings.Ports[\"${INTERNAL_PORT}/tcp\"][0].HostPort // .NetworkSettings.Ports[\"${INTERNAL_PORT}\"][0].HostPort")

    # Check if HOST_PORT was found and is not the string "null"
    if [ "$HOST_PORT" != "null" ] && [ -n "$HOST_PORT" ]; then
        export ${ENV_VAR_NAME}=${HOST_PORT}
        echo "Info: ${ENV_VAR_NAME} set to ${HOST_PORT}"
    else
        echo "Warning: Could not determine host port for internal port ${INTERNAL_PORT}. ${ENV_VAR_NAME} not set."
        # For debugging, you might want to see what was returned:
        # echo "Debug: API Response for ports: $(echo "$API_RESPONSE" | jq '.NetworkSettings.Ports')"
    fi
fi

# Execute the original command (CMD) intended for the container
echo "Info: Executing command: $$@"
exec "$@"