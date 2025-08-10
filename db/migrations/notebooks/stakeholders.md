# Stakeholders

## Introduction

This document contains information about "who or what uses which collections" in the Mongo database.

This information can be used when, for example,
you're preparing to do something that involves a specific collection (e.g. migrating its data to a new schema version),
and you want to ensure that the people that use that collection (i.e. the stakeholders) are aware of your plans.

## Table

Here's a table of (a) names of Mongo collections and (b) the NMDC system components that write to them
(as of
**September 11, 2023**,
according to a
[Slack conversation](https://nmdc-group.slack.com/archives/C01SVTKM8GK/p1694465755802979?thread_ts=1694216327.234519&cid=C01SVTKM8GK)
that took place that day).

| Mongo collection                            | System component                                                            |
|---------------------------------------------|-----------------------------------------------------------------------------|
| `biosample_set`                             | Workflows<br/>(via "manual entry" via Runtime API)                          |
| `data_object_set`                           | Workflows<br/>(via Runtime API)                                             |
| `mags_activity_set`                         | Workflows<br/>(via Runtime API)                                             |
| `metagenome_annotation_activity_set`        | Workflows<br/>(via Runtime API)                                             |
| `metagenome_assembly_set`                   | Workflows<br/>(via Runtime API)                                             |
| `read_based_taxonomy_analysis_activity_set` | Workflows<br/>(via Runtime API)                                             |
| `read_qc_analysis_activity_set`             | Workflows<br/>(via Runtime API)                                             |
| `jobs`                                      | Scheduler<br/>(via MongoDB directly; e.g. `pymongo`)                        |
| `*`                                         | `nmdc-runtime`; i.e. Runtime API<br/>(via MongoDB directly; e.g. `pymongo`) |

