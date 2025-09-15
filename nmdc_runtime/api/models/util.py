from typing import TypeVar, List, Optional, Generic, Annotated

from pydantic import model_validator, Field, BaseModel

ResultT = TypeVar("ResultT")


class ListResponse(BaseModel, Generic[ResultT]):
    resources: List[ResultT]
    next_page_token: Optional[str] = None


class ListRequest(BaseModel):
    r"""
    An encapsulation of a set of parameters accepted by API endpoints related to listing things.

    Note: This class was documented after the `FindRequest` class was documented. You can refer to the documentation of
          the latter class for additional context about the usage of Pydantic's `Field` constructor in this class.
    """

    filter: Optional[str] = Field(
        default=None,
        title="Filter",
        description="""The criteria by which you want to filter the resources, in the same format as the [`query`
                    parameter](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#std-label-method-find-query)
                    of MongoDB's `db.collection.find()` method.\n\n_Example:_
                    `{"lat_lon.latitude": {"$gt": 45.0}, "ecosystem_category": "Plants"}`""",
        examples=[
            r'{"ecosystem_type": "Freshwater"}',
            r'{"lat_lon.latitude": {"$gt": 45.0}, "ecosystem_category": "Plants"}',
        ],
    )
    # TODO: Document the following things about this type hint and `Field` definition:
    #       (a) why the type here is `int` as opposed to `PerPageRange` (`FindRequest` uses the latter),
    #       (b) why the default value here is 20 as opposed to 25 (the default value in `FindRequest`), and
    #       (c) why there is no upper limit on the value (the `PerPageRange` type has an upper limit of 2000).
    #
    # Note: If the HTTP request lacks a value for this parameter, Pydantic will fall back to the default value specified here.
    max_page_size: int = Field(
        default=20,
        title="Resources per page",
        description="How many resources you want _each page_ to contain, formatted as a positive integer.",
        examples=[20],
    )
    page_token: Optional[str] = Field(
        default=None,
        title="Next page token",
        description="""A bookmark you can use to fetch the _next_ page of resources. You can get this from the
                    `next_page_token` field in a previous response from this endpoint.\n\n_Example_: 
                    `nmdc:sys0zr0fbt71`""",
        examples=[
            "nmdc:sys0zr0fbt71",
        ],
    )
    # TODO: Document the endpoint's behavior when a projection includes a _nested_ field identifier (i.e. `foo.bar`),
    #       and ensure the endpoint doesn't break when the projection includes field descriptors that contain commas.
    projection: Optional[str] = Field(
        default=None,
        title="Projection",
        description="""Comma-delimited list of the names of the fields you want the resources in the response to
                    include. Note: In addition to those fields, the response will also include the `id`
                    field.\n\n_Example_: `name, ecosystem_type`""",
        examples=[
            "name, ecosystem_type",
        ],
    )


PerPageRange = Annotated[int, Field(gt=0, le=2_000)]


class FindRequest(BaseModel):
    r"""
    An encapsulation of a set of parameters accepted by API endpoints related to finding things.

    Notes:
    - The "Query Parameter Models" section of the FastAPI docs says that this way of encapsulating
      a set of query parameter definitions in a Pydantic model — so that Swagger UI displays a given
      parameter's _description_ — was introduced in FastAPI 0.115.0.
      Reference: https://fastapi.tiangolo.com/tutorial/query-param-models/
    - While Swagger UI does show the parameter's _description_, specifically, it does not currently show the
      parameter's _title_ or example value(s). The approach shown in the "Classes as Dependencies" section
      of the FastAPI docs (i.e. https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/)
      does result in Swagger UI showing those additional things, but the approach involves not inheriting
      from Pydantic's `BaseModel` class and involves defining an `__init__` method for the class. That is
      further than I want to take these classes from their existing selves at this point. To compensate
      for that, I have included examples _within_ some of the descriptions.
      Reference: https://github.com/fastapi/fastapi/issues/318#issuecomment-507043221
    - The "Fields" section of the Pydantic docs says:
      > "The `Field` function is used to customize and add metadata to fields of models."
      References: https://docs.pydantic.dev/latest/concepts/fields/
    """

    filter: Optional[str] = Field(
        default=None,
        title="Filter",
        description="""The criteria by which you want to filter the resources, formatted as a comma-separated list of
                    `attribute:value` pairs. The `value` can include a comparison operator (e.g. `>=`). If the attribute
                    is of type _string_ and you append `.search` to its name, the server will perform a full-text
                    search.\n\n_Example:_ `ecosystem_category:Plants, lat_lon.latitude:>35.0`""",
        examples=[
            "ecosystem_category:Plants",
            "ecosystem_category:Plants, lat_lon.latitude:>35.0",
        ],
    )
    search: Optional[str] = Field(
        default=None,
        title="Search",
        description="N/A _(not implemented yet)_",
    )
    sort: Optional[str] = Field(
        default=None,
        title="Sort",
        description="""How you want the resources to be ordered in the response, formatted as a comma-separated list of
                    `attribute:value` pairs. Each `attribute` is the name of a field you want the resources to be
                    ordered by, and each `value` is the direction you want the values in that field to be ordered
                    (i.e. `asc` or no value for _ascending_ order, and `desc` for _descending_ order).\n\n_Example:_
                    `depth.has_numeric_value:desc, ecosystem_type`""",
        examples=[
            "depth.has_numeric_value:desc",
            "depth.has_numeric_value:desc, ecosystem_type",
        ],
    )
    page: Optional[int] = Field(
        default=None,
        title="Page number",
        description="""_Which page_ of resources you want to retrieve, when using page number-based pagination.
                    This is the page number formatted as an integer ≥ 1.
                    **Limitation:** When using _page number_-based pagination, only the first 10,000 resources
                    are accessible. You can access resources beyond that by using _cursor_-based pagination.""",
        examples=[1],
    )
    per_page: PerPageRange = Field(
        default=25,
        title="Resources per page",
        description="How many resources you want _each page_ to contain, formatted as a positive integer ≤ 2000.",
        examples=[25],
    )
    cursor: Optional[str] = Field(
        default=None,
        title="Cursor",
        description="""A bookmark you can use to fetch the _next_ page of resources, when using cursor-based pagination.
                    To begin using cursor-based pagination, set the `cursor` parameter to `*`. The response's `meta` object will
                    include a `next_cursor` field, whose value can be used as the `cursor` parameter in a subsequent
                    request.\n\n_Example_: `nmdc:sys0zr0fbt71`""",
        examples=[
            "*",
            "nmdc:sys0zr0fbt71",
        ],
    )
    group_by: Optional[str] = Field(
        default=None,
        title="Group by",
        description="N/A _(not implemented yet)_",
    )
    fields: Optional[str] = Field(
        default=None,
        title="Fields",
        description="""The fields you want the resources to include in the response, formatted as a comma-separated list
                    of field names. This can be used to reduce the size and complexity of the response.\n\n_Example:_
                    `name, ess_dive_datasets`""",
        examples=[
            "name",
            "name, ess_dive_datasets",
        ],
    )

    # Reference: https://docs.pydantic.dev/latest/concepts/validators/#model-validators
    @model_validator(mode="before")
    def set_page_if_cursor_unset(cls, values):
        page, cursor = values.get("page"), values.get("cursor")
        if page is not None and cursor is not None:
            raise ValueError("cannot use cursor- and page-based pagination together")
        if page is None and cursor is None:
            values["page"] = 1
        return values


class FindResponse(BaseModel):
    meta: dict
    results: List[dict]
    group_by: List[dict]


class DeleteResponse(BaseModel):
    r"""
    Response model for "delete" operations. It summarizes the result of the
    operation and it lists identifiers of the documents that were deleted.
    """

    message: str = Field(
        description="Success message describing the deletion operation"
    )
    deleted_workflow_execution_ids: List[str] = Field(
        # Note: `default_factory=list` sets this to an empty list by default.
        default_factory=list,
        description="The `id`s of the `WorkflowExecution`s that were deleted",
    )
    deleted_data_object_ids: List[str] = Field(
        default_factory=list,
        description="The `id`s of the `DataObject`s that were deleted",
    )
    deleted_functional_annotation_agg_oids: List[str] = Field(
        default_factory=list,
        description="The internal MongoDB `ObjectId`s of the `FunctionalAnnotationAggMember`s that were deleted",
    )
    deleted_job_ids: List[str] = Field(
        default_factory=list,
        description="The `id`s of the `jobs` documents that were deleted",
    )


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
    # TODO: Refrain from ensuring indexes exist in the `omics_processing_set` collection,
    #       since that collection was deleted as part of the "Berkeley schema" refactor.
    #       Reference: https://microbiomedata.github.io/nmdc-schema/v10-vs-v11-retrospective/#slots-removed-from-database
    "omics_processing_set": {
        "has_input",
        "has_output",
        "instrument_name",
        "alternative_identifiers",
    },
    "functional_annotation_agg": {"was_generated_by"},
    "workflow_execution_set": {
        "has_input",
        "has_output",
    },
    # Note: The `jobs` collection is not described by the NMDC schema.
    "jobs": {
        "config.activity_id",
    },
}
