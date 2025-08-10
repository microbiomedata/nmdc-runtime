# Contributing to the Docs

Reference for contributing to this documentation.

## Commands

* `mkdocs serve -f docs/mkdocs.yml` - Build the MkDocs documentation website and serve it (with live reloading) at `http://localhost:8000`.
* `mkdocs build -f docs/mkdocs.yml` - Build the MkDocs documentation website.
* `mkdocs -h` - Print help message and exit.

## File tree

```
docs/
    content/           # Documentation source files (e.g. `.md`, `.ipynb`).
        index.md       # The documentation homepage.
        nb/            # Jupyter notebooks (`.ipynb`).
        ...
    mkdocs_overrides/  # Files that override default parts of the theme.
        ...
    mkdocs.yml         # The MkDocs configuration file.
```