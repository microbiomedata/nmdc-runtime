FROM python:3.12-slim-trixie

WORKDIR /app

RUN apt clean && \
    apt-get upgrade && \
    apt-get update

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

# Copy the project files into the image.
COPY . .

# Install the project and its dependencies.
# Docs: https://docs.astral.sh/uv/guides/integration/docker/#installing-a-project
#
# Note: We thought about using `ENV UV_COMPILE_BYTECODE=1` in order to simplify this command,
#       but that made it so that even the subsequent `$ uv run` commands would compile bytecode,
#       defeating the purpose of compiling it up front (we wanted to avoid doing so at run time).
#       There's an open issue about this, here: https://github.com/astral-sh/uv/issues/12202
#
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --compile-bytecode

# Run the app.
# Note: Since we use `ENTRYPOINT` here, container instantiators will be able to specify
#       app CLI options to the `$ docker run` command without repeating the app name.
ENTRYPOINT ["uv", "run", "nmdc-migration-cli"]
