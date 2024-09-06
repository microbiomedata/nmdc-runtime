from typing import TypeVar, List, Optional, Generic, Annotated

from fastapi import Query

from pydantic import model_validator, Field, BaseModel

ResultT = TypeVar("ResultT")


class ListResponse(BaseModel, Generic[ResultT]):
    resources: List[ResultT]
    next_page_token: Optional[str] = None


class ListRequest(BaseModel):
    filter: Annotated[
        Optional[str],
        Query(
            description='MongoDB-style JSON filter document. Example: `{"ecosystem_type": "Freshwater"}`'
        ),
    ] = None
    max_page_size: Optional[int] = 20
    page_token: Optional[str] = None
    projection: Annotated[
        Optional[str],
        Query(
            description=(
                "for MongoDB-like "
                "[projection](https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/): "
                "comma-separated list of fields you want the objects in the response to include. "
                "Note: `id` will always be included. "
                "Example: `ecosystem_type,name`"
            )
        ),
    ] = None


PerPageRange = Annotated[int, Field(gt=0, le=2_000)]


class FindRequest(BaseModel):
    filter: Optional[str] = None
    search: Optional[str] = None
    sort: Optional[str] = None
    page: Optional[int] = None
    per_page: Optional[PerPageRange] = 25
    cursor: Optional[str] = None
    group_by: Optional[str] = None
    fields: Annotated[
        Optional[str],
        Query(
            description="comma-separated list of fields you want the objects in the response to include"
        ),
    ] = None

    @model_validator(mode="before")
    def set_page_if_cursor_unset(cls, values):
        page, cursor = values.get("page"), values.get("cursor")
        if page is not None and cursor is not None:
            raise ValueError("cannot use cursor- and page-based pagination together")
        if page is None and cursor is None:
            values["page"] = 1
        return values


class FindApiRequest:
    r"""
    This class encapsulates a set of parameters accepted by API endpoints related to finding things.

    Note: It does not inherit from Pydantic's `BaseModel` class; because FastAPI cannot extract
          descriptions from such classes, per https://github.com/fastapi/fastapi/issues/318.

    Reference: https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/

    See also: https://docs.pydantic.dev/latest/concepts/validators/#validation-context
    """
    def __init__(
        self,
        filter: Annotated[Optional[str], Query(
            title="Filter",
            description="" +
                r"""Specifies conditions for the query, returning only documents that satisfy the conditions.
                <br /><br />
                Comma-separated list of `attribute:value` pairs.
                The value can include a comparison operator, such as `>=`, `<=`, `<`, or `>`.
                May use a `.search` after the attribute name to conduct a full text search if field is of type "string".
                <br /><br />
                Syntax: `attribute:value, attribute.search:value`
                <br /><br />
                """,
            example=r"ecosystem_category:Plants, lat_lon.latitude:>35.0",
        )] = None,
        search: Annotated[Optional[str], Query(
            title="Search",
            description="Not implemented yet",
        )] = None,
        sort: Annotated[Optional[str], Query(
            title="Sort",
            description="" +
                r"""Specifies the order in which the query returns the matching documents.
                <br /><br />
                Comma-separated list of `attribute:value` pairs, where the value can be
                empty, `asc`, or `desc` (for ascending or descending order).
                <br /><br />
                Syntax: `attribute` or `attribute:asc` or `attribute:desc`
                <br /><br />
                """,
            example=r"depth.has_numeric_value:desc, ecosystem_type",
        )] = None,
        page: Annotated[Optional[int], Query(
            title="Page number",
            description="" +
                r"""Specifies the desired page number among the paginated results.
                <br /><br />
                Syntax: Integer ≥ 1
                <br /><br />
                """,
            example=r"3",
        )] = None,
        per_page: Annotated[Optional[PerPageRange], Query(
            title="Documents per page",
            description="" +
                r"""Specifies the number of documents returned per page.
                <br /><br />
                Syntax: Integer ≤ 2000
                <br /><br />
                """,
            example=r"50",
        )] = 25,
        cursor: Annotated[Optional[int], Query(
            title="Cursor",
            description="" +
                r"""A bookmark for where a query can pick up where it has left off.
                To use cursor paging, set the `cursor` parameter to `*`.
                The response's `meta` object will include a `next_cursor` value
                that can be used as a `cursor` parameter to retrieve the next page of results.
                <br /><br />
                Syntax: Either `*` or the value of `next_cursor`
                <br /><br />
                """,
            example=r"nmdc:sys0zr0fbt71",
        )] = None,
        group_by: Annotated[Optional[str], Query(
            title="Group by",
            description="Not implemented yet",
        )] = None,
        fields: Annotated[Optional[str], Query(
            title="Fields",
            description="" +
                r"""Indicates the desired fields to be included in the response.
                Helpful for trimming down the returned results.
                Comma-separated list of fields that belong to the documents in the collection being queried.
                <br /><br />
                Syntax: `field1[, field2, ...]`
                <br /><br />
                """,
            example=r"name, ess_dive_datasets",
        )] = None,
    ):
        self.filter = filter
        self.search = search
        self.sort = sort
        self.page = page
        self.per_page = per_page
        self.cursor = cursor
        self.group_by = group_by
        self.fields = fields

        self._set_page_if_neither_page_nor_cursor_is_set()

    def _set_page_if_neither_page_nor_cursor_is_set(self):
        r"""
        Sets the page if neither the page nor the cursor is set; raising an exception if both are set.
        """
        if self.page is None and self.cursor is None:
            self.page = 1
        elif self.page is not None and self.cursor is not None:
            raise ValueError("Cannot use both cursor-based and page-based pagination simultaneously.")


class PipelineFindRequest(BaseModel):
    pipeline_spec: str
    description: str


class FindResponse(BaseModel):
    meta: dict
    results: List[dict]
    group_by: List[dict]


class PipelineFindResponse(BaseModel):
    meta: dict
    results: List[dict]


# Note: For MongoDB, a single collection can have no more than 64 indexes
# Note: Each collection has a unique index set on "id" elsewhere.
entity_attributes_to_index = {
    "biosample_set": {
        "alternative_identifiers",
        "env_broad_scale.has_raw_value",
        "env_local_scale.has_raw_value",
        "env_medium.has_raw_value",
        "collection_date.has_raw_value",
        "ecosystem",
        "ecosystem_category",
        "ecosystem_type",
        "ecosystem_subtype",
        "specific_ecosystem",
        # Note: if `lat_lon` was GeoJSON, i.e. {type,coordinates}, MongoDB has a "2dsphere" index
        "lat_lon.latitude",
        "lat_lon.longitude",
    },
    "study_set": {
        "has_credit_associations.applied_roles",
        "has_credit_associations.applies_to_person.name",
        "has_credit_associations.applies_to_person.orcid",
    },
    "data_object_set": {
        "data_object_type",
        "file_size_bytes",
        "md5_checksum",
        "url",
    },
    "omics_processing_set": {
        "has_input",
        "has_output",
        "instrument_name",
        "alternative_identifiers",
    },
}
