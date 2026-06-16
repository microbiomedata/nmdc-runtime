# Concise reference for AI agents

<!-- Reference: https://agents.md -->

## Repository structure

- `.docker/`: Files supporting container-based development and testing.
- `.github/`: GitHub Actions workflows, issue templates, and PR templates. (YAML, Markdown)
- `db/`: CLI app and legacy notebooks related to Mongo database migration. (Python, Typer). **Not part of main application.**
- `docs/`: User-facing and developer-facing documentation about the main application. (Material for MkDocs)
- `nmdc_runtime/`: Main application. (Python, FastAPI)
- `nmdc_runtime/minter`: Part of the main application responsible for minting unique identifiers. May eventually be extracted into a separate API.
- `tests/`: Tests targeting main application. (pytest)
- `util/`: Scripts that facilitate development. (Python, bash). **Not part of main application.**
- `docker-compose.test.yml`: Docker Compose configuration for testing stack.
- `docker-compose.yml`: Docker Compose configuration for development stack.
- `Makefile`: CLI recipes for common tasks.
- `pyproject.toml`: Project configuration and dependency specifications.
- `uv.lock`: Locked list of dependencies.
- `README.md`: Concise developer-facing documentation.
- `RELEASES.md`: Obsolete file previously used to log changes.

## Main application

### Overview

- The main application is a FastAPI app that provides HTTP API endpoints people can use to access the NMDC MongoDB database, named "nmdc".
- The user base consists of microbiome researchers, data scientists, software developers, biologists, and bioinformaticians.
- The main application is colloquially referred to as "the Runtime" (not "run-time" or "run time") or "NMDC Runtime".
- The dependency management tool is `uv`.
- The repository may contain some obsolete code and/or documentation.

### Main technologies

- Python
- FastAPI
- MongoDB
- Dagster

## Before making changes

- Avoid broad refactors unless instructed otherwise.
- Make it easy for humans to review the changes you make.

## Commands

### Development

- `make dev`: Start the development stack using Docker Compose.
- `make black`: Format source code in `nmdc_runtime/`.
- `make lint`: Lint source code in `nmdc_runtime/` and `tests/`.

#### Dependency management

- `docker compose run --rm --no-deps fastapi sh -c 'uv sync --active'`: Synchronize transitive dependencies after updating `pyproject.toml`.

### Testing

Test all changes that you make, unless instructed otherwise.

- `make test`: Spin up testing stack and run full test suite.
- `make test ARGS='{additional pytest arguments}'`: Same, but with additional arguments passed to pytest. Can be used to run specific test(s).
- `make test-shell`: Spin up testing stack and start a shell in the test container. Useful for rapid TDD.
