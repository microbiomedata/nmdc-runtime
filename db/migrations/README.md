# Migrations

This directory contains files related to migrating the MongoDB database.

It has the following subdirectories:

- `./notebooks`: Python notebooks related to migrating the MongoDB database between versions of the NMDC Schema. This notebook-based approach has been superseded by the CLI-based approach (see below).
- `./ad_hoc_scripts`: MongoDB (`mongosh`) scripts—and maybe, eventually, other kinds of scripts—related to migrating parts of the MongoDB database that aren't described by the NMDC Schema.
- `./cli`: Command-line application people can use to run migrators. This is the successor to the notebook-based approach (see above).
