FROM mongo:8.2.3

# Define environment variables that are meaningful to the app.
#
# Note: These can be overridden in two ways: (a) to override the environment variables, themselves,
#       you can use the `-e KEY=VALUE` option with `$ docker run`; and (b) to override them as
#       parameters to the app, you can pass the corresponding CLI options to the app, since the CLI
#       options takes precedence over the environment variables.
#
ENV MONGODUMP_PATH="/usr/bin/mongodump"
ENV MONGORESTORE_PATH="/usr/bin/mongorestore"
ENV ORIGIN_DUMP_FOLDER_PATH="/tmp/mongodump.origin.out"
ENV TRANSFORMER_DUMP_FOLDER_PATH="/tmp/mongodump.transformer.out"

# Define environment variables that are meaningful to the init shell script defined below.
ENV MONGO_LOG_PATH="/tmp/mongo.log"

WORKDIR /app

# Install `git`.
RUN apt clean && \
    apt-get upgrade && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        git

# Install `uv` (the `uv` docs recommend pinning to a specific `uv` version, so we have done that).
# Docs: https://docs.astral.sh/uv/guides/integration/docker/#installing-uv
COPY --from=ghcr.io/astral-sh/uv:0.11.11 /uv /uvx /bin/

# Configure `uv` to not install development dependencies
# Docs: https://docs.astral.sh/uv/guides/integration/docker/#installing-a-project
ENV UV_NO_DEV=1

# Configure how `uv` makes cached packages available to Python.
# Note: The default value is `clone`, but I see "warning: Failed to hardlink files"
#       when building the image that way. Also, the example in the `uv` containerization
#       shows `copy`.
# Docs: https://docs.astral.sh/uv/guides/integration/docker/#caching
ENV UV_LINK_MODE=copy

# Copy the dependency lists into the image.
COPY pyproject.toml .
COPY uv.lock .

# Install the project's production dependencies.
# Docs: https://docs.astral.sh/uv/guides/integration/docker/#installing-a-project
#
# Note: We thought about using `ENV UV_COMPILE_BYTECODE=1` in order to simplify this command,
#       but that made it so that even the subsequent `$ uv run` commands would compile bytecode,
#       defeating the purpose of compiling it up front (we wanted to avoid doing so at run time).
#       There's an open issue about this, here: https://github.com/astral-sh/uv/issues/12202
#
# Note: We opted to _not_ install the project, itself, at this stage. That is so developers are free
#       to edit project files (other than those already copied into the image) without invalidating
#       the layer cache. We'll install the project, itself, later (i.e. further below).
#       Docs: https://docs.astral.sh/uv/concepts/projects/sync/#partial-installations
#
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --compile-bytecode \
        --no-dev --no-install-project

# Upgrade `pip` to the latest version.
#
# Note: We also set pip's cache directory to one that the user our app runs as has write access to
#       (by default, pip's cache location is `/data/db/.cache/pip`, which that user doesn't have
#       write access to, resulting in some run-time warnings).
#
ENV PIP_CACHE_DIR="/tmp/.pip_cache_dir"
RUN uv pip install --upgrade pip

# Create an init shell script that does two things: (1) it launches MongoDB via the same entrypoint
# script as the base image does, except it launches it in daemon mode and configures it to write
# its logs to a file instead of to the console; and (2) it executes, on the shell, as the main
# process of the container, whatever arguments were passed to the init script.
RUN echo '#! /bin/bash'                                                      > /init.sh && \
    echo 'echo "\nLaunching MongoDB in daemon mode.\n"'                     >> /init.sh && \
    echo 'docker-entrypoint.sh mongod --fork --logpath "${MONGO_LOG_PATH}"' >> /init.sh && \
    echo 'echo "\nExecuting command.\n"'                                    >> /init.sh && \
    echo 'exec "$@"'                                                        >> /init.sh && \
    chmod +x                                                                   /init.sh

# Copy the project files into the image.
COPY src src
COPY README.md README.md

# Install the project, itself.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --compile-bytecode

# For convenience during development, install `nmdc-schema` into the Python virtual environment,
# without adding add it to the `pyproject.toml` or `uv.lock` files. Developers can tell the app to
# use it, via `nmdc-migration-cli migrate --migrator-git-tag '-INSTALLED' ...`. In production, users
# will typically set `--migrator-git-tag` to a specific Git tag.
RUN uv pip install nmdc-schema

# Run the init shell script such that it runs our app.
ENTRYPOINT ["/init.sh", "uv", "run", "nmdc-migration-cli"]
CMD ["--help"]
