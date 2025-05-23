# Improving the Search API

A standard operating procedure (SOP) for search API maintainers/contributors.

The "Search API" may be thought of that part of the Runtime API that is both read-only and is
particular to the needs of (meta)data consumers to retrieve NMDC data products that conform to the
NMDC schema.

The "Data Management API", on the other hand, is all other aspects of the runtime API -- that is,
the read-write parts that also serve the needs of data producers and data stewards.

## Endpoints

Endpoints for the Search API are defined in the
[`nmdc_runtime.api.endpoints.find`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/api/endpoints/find.py)
module.

To add an endpoint, you will likely use the `FindRequest` and `FindResponse` models in
[`nmdc_runtime.api.models.util`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/api/models/util.py),
as they are the shared request/response models used by existing Search API endpoints.

You will also likely use the `find_resources` helper function from
[`nmdc_runtime.api.endpoints.util`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/api/endpoints/util.py).
Improvements to this helper function will improve all existing Search API endpoints.

Adding a dedicated endpoint for a particular resource collection may be as simple as copying the
code for a representative pair of endpoints, such as `GET /studies` and `GET /studies/{study_id}`,
and changing names accordingly.

## Index-backed Filter Attributes

In order to ensure an index for a particular attribute/slot of a collection entity, add it to the
`entity_attributes_to_index` dictionary in the
[`nmdc_runtime.api.models.util`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/api/models/util.py)
module. Each key of that dictionary is the collection name for the entity, e.g. `biosample_set`, and
each value corresponding to a key is the set of attributes, e.g. `ecosystem` and
`collection_date.has_raw_value`, for which an index will be ensured.

Currently, due to the limitations of the database technology (MongoDB) that backs the API, a single
collection can have no more than 64 indexes.

When the API server code is re-deployed, the `ensure_indexes` startup hook in the
[`nmdc_runtime.api.main`](https://github.com/microbiomedata/nmdc-runtime/blob/main/nmdc_runtime/api/main.py)
module is run, which fetches `entity_attributes_to_index` and ensures the corresponding indexes
exist.


