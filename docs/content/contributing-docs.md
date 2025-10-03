# Contributing to the Docs

Reference for contributing to this documentation.

## Quick start

1. Set up Python virtual environment that includes docs-related dependencies.

   ```shell
   uv sync --group docs
   ```

2. Start the [MkDocs development server](https://www.mkdocs.org/user-guide/cli/#mkdocs-serve).

   ```shell
   uv run --no-sync mkdocs serve -f docs/mkdocs.yml
   ```

   > The MkDocs development server will be accessible at: [`http://localhost:8000`](http://localhost:8000). It will have live reloading enabled. You can press `Ctrl+C` to terminate the server.

3. (Optional) Build the MkDocs website.

   ```shell
   uv run --no-sync mkdocs build -f docs/mkdocs.yml
   ```

## File tree

```console
docs/
    content/           # Documentation source files (e.g. `.md`, `.ipynb`).
        index.md       # The documentation homepage.
        nb/            # Jupyter notebooks (`.ipynb`).
        ...
    mkdocs_overrides/  # Files that override default parts of the theme.
        ...
    mkdocs.yml         # The MkDocs configuration file.
```
