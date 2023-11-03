"""API endpoints."""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pymongo.database import Database as MongoDatabase
from nmdc_schema.nmdc_data import get_nmdc_file_type_enums
from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module

from nmdc_runtime.api.endpoints.util import strip_oid_recursive
from nmdc_runtime.api.models.user import get_current_active_user, User
from nmdc_runtime.api.db.mongo import get_mongo_db

router = APIRouter()

BIOSAMPLE_SEARCH_COLLECTION = "biosample_denormalized"
OMICS_PROCESSING_SEARCH_COLLECTION = "omics_processing_denormalized"


# get application settings
# @router.get("/settings", name="Get application settings")
# async def get_settings() -> Dict[str, Any]:
#     settings = Settings()
#     return {"disable_bulk_download": settings.disable_bulk_download.upper() == "YES"}


# get the current user information
@router.get("/me", tags=["user"], name="Return the current user name")
async def me(
    _request: Request, user: str = Depends(get_current_active_user)
) -> Optional[str]:
    return user


# @router.get("/me/orcid", tags=["user"], name="Return the ORCID iD of current user")
# async def my_orcid(
#     _request: Request,
#     orcid: str = Depends(get_current_user_orcid)
# ) -> Optional[str]:
#     return orcid


# autocomplete search
# @router.get("/search", tags=["aggregation"], response_model=List[query.ConditionResultSchema])
# def text_search(terms: str, limit=6, db: Session = Depends(get_db)):
#     return crud.text_search(db, terms, limit)


# database summary
@router.get(
    "/summary",
    tags=["aggregation"],
)
async def get_database_summary():
    """Return types and value ranges for each field."""
    return {
        "biosample": {
            "attributes": {
                "depth.has_numeric_value": {
                    "min": 0,
                    "max": 2000,
                    "type": "float",
                },
                "geo_loc_name.has_raw_value": {
                    "type": "string",
                },
                "gold_classification": {
                    "type": "sankey-tree",
                },
                "env_broad_scale.term.id": {
                    "type": "tree",
                },
                "env_local_scale.term.id": {
                    "type": "tree",
                },
                "env_medium.term.id": {
                    "type": "tree",
                },
                "lat_lon.latitude": {
                    "type": "float",
                    "min": -90,
                    "max": 90,
                },
                "lat_lon.longitude": {
                    "type": "float",
                    "min": -180,
                    "max": 180,
                },
                "collection_date.has_date_value": {
                    "type": "date",
                    "min": "2000-03-15T00:00:00",
                    "max": "2022-08-12T00:00:00",
                },
            },
        },
        "gene_function": {
            "attributes": {
                "id": {
                    "type": "kegg_search",
                },
            },
        },
        "omics_processing": {
            "attributes": {
                "omics_type.has_raw_value": {
                    "type": "string",
                },
                "instrument_name": {
                    "type": "string",
                },
                "processing_institution": {
                    "type": "string",
                },
            },
        },
        "study": {
            "attributes": {
                "principal_investigator.has_raw_value": {
                    "type": "string",
                },
            },
        },
    }


from datetime import datetime
from typing import Annotated, List, Literal, Optional, Union


AnnotationValue = Union[float, int, datetime, str, dict, list]


class Table(Enum):
    biosample = "biosample"
    study = "study"
    omics_processing = "omics_processing"
    reads_qc = "reads_qc"
    metagenome_assembly = "metagenome_assembly"
    metagenome_annotation = "metagenome_annotation"
    metaproteomic_analysis = "metaproteomic_analysis"
    mags_analysis = "mags_analysis"
    nom_analysis = "nom_analysis"
    read_based_analysis = "read_based_analysis"
    metabolomics_analysis = "metabolomics_analysis"
    metatranscriptome = "metatranscriptome"
    gene_function = "gene_function"
    metap_gene_function = "metap_gene_function"
    data_object = "data_object"

    env_broad_scale = "env_broad_scale"
    env_local_scale = "env_local_scale"
    env_medium = "env_medium"

    principal_investigator = "principal_investigator"


# Custom exceptions to provide better error responses in the API.
class InvalidAttributeException(Exception):
    def __init__(self, table: str, attribute: str):
        self.table = table
        self.attribute = attribute
        super(InvalidAttributeException, self).__init__(
            f"Attribute {self.attribute} not found in table {self.table}"
        )


class InvalidFacetException(Exception):
    pass


class Operation(Enum):
    equal = "=="
    greater = ">"
    greater_equal = ">="
    less = "<"
    less_equal = "<="
    not_equal = "!="


NumericValue = Union[float, int, datetime]
RangeValue = Annotated[List[AnnotationValue], Field(min_items=2, max_items=2)]


class GoldTreeValue(BaseModel):
    ecosystem: Optional[str]
    ecosystem_category: Optional[str]
    ecosystem_type: Optional[str]
    ecosystem_subtype: Optional[str]
    specific_ecosystem: Optional[str]


ConditionValue = Union[AnnotationValue, RangeValue, List[GoldTreeValue]]


class BaseConditionSchema(BaseModel):
    field: str
    value: ConditionValue
    table: Table


# This condition type represents the original DSL for comparisons.  It
# represents simple binary operators on scalar fields and is generally
# compatible with all attributes.
class SimpleConditionSchema(BaseConditionSchema):
    op: Operation = Operation.equal
    field: str
    value: AnnotationValue
    table: Table


# A range query that can't be achieved with simple conditions (because they are "or"-ed together).
class RangeConditionSchema(BaseConditionSchema):
    op: Literal["between"]
    field: str
    value: RangeValue
    table: Table


# A special condition type used on gold terms that supports hierarchical queries.
class GoldConditionSchema(BaseConditionSchema):
    table: Table  # can't do a Literal on an enum type
    value: List[GoldTreeValue]
    field: Literal["gold_tree"]
    op: Literal["tree"]


# A special condition type on multiomics bitstrings
class MultiomicsConditionSchema(BaseConditionSchema):
    table: Table
    value: Union[int, List[str]]
    field: Literal["multiomics"]
    op: Literal["has"]


ConditionSchema = Union[
    RangeConditionSchema,
    SimpleConditionSchema,
    GoldConditionSchema,
    MultiomicsConditionSchema,
]


class BaseQuerySchema(BaseModel):
    conditions: List[ConditionSchema] = []


class WorkflowActivityTypeEnum(Enum):
    reads_qc = "nmdc:ReadQCAnalysisActivity"
    metagenome_assembly = "nmdc:MetagenomeAssembly"
    metagenome_annotation = "nmdc:MetagenomeAnnotation"  # TODO name out of date, fix
    metaproteomic_analysis = "nmdc:MetaProteomicAnalysis"
    mags_analysis = "nmdc:MAGsAnalysisActivity"
    read_based_analysis = "nmdc:ReadbasedAnalysis"  # TODO name out of date, fix
    nom_analysis = "nmdc:NomAnalysisActivity"
    metabolomics_analysis = "nmdc:MetabolomicsAnalysisActivity"
    raw_data = "nmdc:RawData"
    metatranscriptome = "nmdc:metaT"


class DataObjectFilter(BaseModel):
    workflow: Optional[WorkflowActivityTypeEnum]
    file_type: Optional[str]


class BiosampleQuerySchema(BaseQuerySchema):
    data_object_filter: List[DataObjectFilter] = []


class DataObjectAggregation(BaseModel):
    count: int
    size: int


class DataObjectQuerySchema(BaseQuerySchema):
    data_object_filter: List[DataObjectFilter] = []


class BaseSearchResponse(BaseModel):
    count: int


class SearchQuery(BaseModel):
    conditions: List[ConditionSchema] = []


class FacetQuery(SearchQuery):
    attribute: str


class BiosampleSearchQuery(SearchQuery):
    data_object_filter: List[DataObjectFilter] = []


class KeggTermText(BaseModel):
    term: str
    text: str


class KeggTermTextListResponse(BaseModel):
    terms: List[KeggTermText]


class EnvoTreeNode(BaseModel):
    id: str
    label: str
    children: List["EnvoTreeNode"]


class EnvoTreeResponse(BaseModel):
    trees: Dict[str, List[EnvoTreeNode]]


class Contributor(BaseModel):
    name: str
    orcid: str
    roles: List[str]


class StudyForm(BaseModel):
    studyName: str
    piName: str
    piEmail: str
    piOrcid: str
    linkOutWebpage: List[str]
    studyDate: Optional[str]
    description: str
    notes: str
    contributors: List[Contributor]


class MultiOmicsForm(BaseModel):
    alternativeNames: List[str]
    studyNumber: str
    GOLDStudyId: str
    JGIStudyId: str
    NCBIBioProjectId: str
    omicsProcessingTypes: List[str]


class NmcdAddress(BaseModel):
    name: str
    email: str
    phone: str
    line1: str
    line2: str
    city: str
    state: str
    postalCode: str


class AddressForm(BaseModel):
    shipper: NmcdAddress
    expectedShippingDate: Optional[datetime]
    shippingConditions: str
    sample: str
    description: str
    experimentalGoals: str
    randomization: str
    usdaRegulated: Optional[bool]
    permitNumber: str
    biosafetyLevel: str
    irbOrHipaa: Optional[bool]
    comments: str


class ContextForm(BaseModel):
    datasetDoi: str
    dataGenerated: Optional[bool]
    facilityGenerated: Optional[bool]
    facilities: List[str]
    award: Optional[str]
    otherAward: str


class MetadataSubmissionRecord(BaseModel):
    packageName: str
    contextForm: ContextForm
    addressForm: AddressForm
    templates: List[str]
    studyForm: StudyForm
    multiOmicsForm: MultiOmicsForm
    sampleData: Dict[str, List[Any]]


class SubmissionMetadataSchemaCreate(BaseModel):
    metadata_submission: MetadataSubmissionRecord
    status: Optional[str]


class SubmissionMetadataSchema(SubmissionMetadataSchemaCreate):
    # id: UUID
    # author_orcid: str
    created: datetime
    status: str
    author: User


class MetadataSubmissionResponse(BaseSearchResponse):
    results: List[SubmissionMetadataSchema]


@router.post(
    "/environment/sankey",
    tags=["aggregation"],
)
async def get_environmental_sankey(
    biosample_query: BiosampleQuerySchema = BiosampleQuerySchema(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Retrieve biosample counts by ecosystem used as input for a sankey diagram."""
    mongo_filter = conditions_to_mongo_filter(biosample_query.conditions)
    results = mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(
        [
            {
                "$match": mongo_filter,
            },
            {
                "$group": {
                    "_id": {
                        "ecosystem": "$ecosystem",
                        "ecosystem_category": "$ecosystem_category",
                        "ecosystem_subtype": "$ecosystem_subtype",
                        "ecosystem_type": "$ecosystem_type",
                        "specific_ecosystem": "$specific_ecosystem",
                    },
                    "count": {
                        "$count": {},
                    },
                },
            },
            {
                "$set": {
                    "_id.count": "$count",
                },
            },
            {
                "$replaceRoot": {
                    "newRoot": "$_id",
                },
            },
        ]
    )
    return strip_oid_recursive(results)


@router.post(
    "/environment/geospatial",
    tags=["aggregation"],
)
async def get_environmental_geospatial(
    biosample_query: BiosampleQuerySchema = BiosampleQuerySchema(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Return samples binned by lat/lon and ecosystem type."""
    mongo_filter = conditions_to_mongo_filter(biosample_query.conditions)
    results = mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(
        [
            {
                "$match": mongo_filter,
            },
            {
                "$group": {
                    "_id": {
                        "latitude": "$lat_lon.latitude",
                        "longitude": "$lat_lon.longitude",
                        "ecosystem": "$ecosystem",
                        "ecosystem_category": "$ecosystem_category",
                    },
                    "count": {
                        "$count": {},
                    },
                },
            },
            {
                "$set": {
                    "_id.count": "$count",
                },
            },
            {
                "$replaceRoot": {
                    "newRoot": "$_id",
                },
            },
        ]
    )
    return strip_oid_recursive(list(results))


def facet_value_to_key(value):
    """Convert a value to a key for use in a facet counting response."""
    if isinstance(value, list):
        return ";".join(value)
    return value


def conditions_to_mongo_filter(conditions, base_type="biosample"):
    """Convert query conditions to a MongoDB filter."""
    mongo_filter = dict()
    for condition in conditions:
        if condition.table.name == base_type:
            field_name = condition.field
        else:
            field_name = f"{condition.table.name}.{condition.field}"

        if condition.op == Operation.equal:
            if not mongo_filter.get(field_name):
                mongo_filter[field_name] = {"$in": []}
            mongo_filter[field_name]["$in"].append(condition.value)
        elif condition.op == Operation.less:
            mongo_filter[field_name] = {"$lt": condition.value}
        elif condition.op == Operation.less_equal:
            mongo_filter[field_name] = {"$lte": condition.value}
        elif condition.op == Operation.greater:
            mongo_filter[field_name] = {"$gt": condition.value}
        elif condition.op == Operation.greater_equal:
            mongo_filter[field_name] = {"$gte": condition.value}
        elif condition.op == "between":
            mongo_filter[field_name] = {
                "$gte": condition.value[0],
                "$lte": condition.value[1],
            }
        elif condition.op == "has":
            mongo_filter[field_name] = {"$all": condition.value}

    return mongo_filter


class Pagination:
    """
    This class is responsible for generating paged responses from sqlalchemy queries.
    """

    DEFAULT_OFFSET = 0
    DEFAULT_LIMIT = 25

    def __init__(
        self,
        request: Request,
        response: Response,
        offset: int = Query(default=DEFAULT_OFFSET, ge=0),
        limit: int = Query(default=DEFAULT_LIMIT, ge=1),
    ):
        self._request = request
        self._response = response
        self.offset = offset
        self.limit = limit


@router.post(
    "/biosample/search",
    tags=["biosample"],
    name="Search for biosamples",
    description="Faceted search of biosample data.",
)
async def search_biosample(
    biosample_query: BiosampleSearchQuery = BiosampleSearchQuery(),
    mdb: MongoDatabase = Depends(get_mongo_db),
    pagination: Pagination = Depends(),
):
    """Search for biosamples."""
    mongo_filter = conditions_to_mongo_filter(biosample_query.conditions)
    aggregation = [
        {
            "$match": mongo_filter,
        },
        {
            "$unset": "gene_function",
        },
        {
            "$sort": {"multiomics_count": -1},
        },
    ]

    def data_object_is_selected(data_object):
        for condition in biosample_query.data_object_filter:
            if (
                data_object.get("data_object_type") == condition.file_type
                and data_object.get("activity_type") == condition.workflow.value
            ):
                return True
        return False

    file_type_map: Dict[str, Tuple[str, str]] = {}
    for val in get_nmdc_file_type_enums():
        file_type_map[val["name"]] = val["description"]

    def add_data_object_selection_and_type(sample):
        for obj in sample["data_object"]:
            obj["data_object_type_description"] = file_type_map.get(
                obj.get("data_object_type", None), None
            )
            obj["selected"] = data_object_is_selected(obj)
        return sample

    return {
        "count": list(
            mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(
                [*aggregation, {"$count": "count"}]
            )
        )[0]["count"],
        "results": [
            strip_oid_recursive(add_data_object_selection_and_type(doc))
            for doc in mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(
                [
                    *aggregation,
                    {"$skip": pagination.offset},
                    {"$limit": pagination.limit},
                ]
            )
        ],
    }


@router.post(
    "/biosample/facet",
    tags=["biosample"],
    name="Get all values of an attribute",
)
async def facet_biosample(
    facet_query: FacetQuery,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Get all values of an attribute."""
    aggregation = [
        {
            "$match": conditions_to_mongo_filter(facet_query.conditions),
        },
        {
            "$sortByCount": f"${facet_query.attribute}",
        },
    ]

    return strip_oid_recursive(
        {
            "facets": {
                facet_value_to_key(facet["_id"]): facet["count"]
                for facet in mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(aggregation)
            },
        }
    )


@router.post(
    "/biosample/binned_facet",
    tags=["biosample"],
    name="Get all values of an attribute",
)
async def binned_facet_biosample(
    facet_query: FacetQuery,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Get all values of an attribute."""
    aggregation = [
        {
            "$match": conditions_to_mongo_filter(facet_query.conditions),
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$collection_date.has_date_value"},
                    "month": {"$month": "$collection_date.has_date_value"},
                },
                "count": {"$count": {}},
            },
        },
    ]

    def date_string(doc):
        return f"{doc['_id']['year']}-{str(doc['_id']['month']).zfill(2)}-01"

    binned_data = list(mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(aggregation))
    binned_data.sort(key=date_string)
    binned_data = [d for d in binned_data if d["_id"]["year"] is not None]

    # Fill in missing months with zero counts
    def next_month(doc):
        if doc["month"] < 12:
            return {"month": doc["month"] + 1, "year": doc["year"]}
        return {"month": 1, "year": doc["year"] + 1}

    full_binned_data: List[Any] = []
    for doc in binned_data:
        if len(full_binned_data) == 0:
            full_binned_data.append(doc)
            continue
        while (
            full_binned_data[-1]["_id"]["year"] != doc["_id"]["year"]
            or full_binned_data[-1]["_id"]["month"] != doc["_id"]["month"]
        ):
            full_binned_data.append(
                {"_id": next_month(full_binned_data[-1]["_id"]), "count": 0}
            )
        full_binned_data[-1] = doc

    # Add one more month with zero count so we have a bin end boundary for the last bin
    full_binned_data.append(
        {"_id": next_month(full_binned_data[-1]["_id"]), "count": 0}
    )
    return strip_oid_recursive(
        {
            "bins": [date_string(d) for d in full_binned_data],
            "facets": [d["count"] for d in full_binned_data][:-1],
        }
    )


@router.get(
    "/biosample/{biosample_id}",
    tags=["biosample"],
)
async def get_biosample(
    biosample_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Get a biosample by ID."""
    biosamples = list(mdb[BIOSAMPLE_SEARCH_COLLECTION].find({"id": biosample_id}))
    if len(biosamples) == 0:
        raise HTTPException(status_code=404, detail="Biosample not found")

    return strip_oid_recursive(biosamples[0])


# @router.get(
#     "/envo/tree",
#     response_model=EnvoTreeResponse,
#     tags=["envo"],
# )
# async def get_envo_tree():
#     return EnvoTreeResponse(trees=nested_envo_trees())


# @router.get(
#     "/kegg/module/{module}",
#     response_model=schemas.KeggTermListResponse,
#     tags=["kegg"],
# )
# async def get_kegg_terms_for_module(module: str, db: Session = Depends(get_db)):
#     terms = crud.list_ko_terms_for_module(db, module)
#     return schemas.KeggTermListResponse(terms=terms)


# @router.get(
#     "/kegg/pathway/{pathway}",
#     response_model=schemas.KeggTermListResponse,
#     tags=["kegg"],
# )
# async def get_kegg_terms_for_pathway(pathway: str, db: Session = Depends(get_db)):
#     terms = crud.list_ko_terms_for_pathway(db, pathway)
#     return schemas.KeggTermListResponse(terms=terms)


@router.get(
    "/kegg/term/search",
    response_model=KeggTermTextListResponse,
    tags=["kegg"],
)
async def kegg_text_search(
    query: str,
    limit=20,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Search for KEGG terms by text."""
    return KeggTermTextListResponse(
        terms=[
            KeggTermText(
                term="K00001",
                text="E1.1.1.1, adh; alcohol dehydrogenase [EC:1.1.1.1]",
            )
        ]
    )


@router.post(
    "/study/search",
    tags=["study"],
    name="Search for studies",
    description="Faceted search of study data.",
)
async def search_study(
    study_query: BiosampleSearchQuery = BiosampleSearchQuery(),
    mdb: MongoDatabase = Depends(get_mongo_db),
    pagination: Pagination = Depends(),
):
    """Search for studies."""
    aggregation = [
        {
            "$match": conditions_to_mongo_filter(study_query.conditions),
        },
        {
            "$unwind": {"path": "$study"},
        },
        {
            "$group": {
                "_id": "$study.id",
                "study": {"$first": "$study"},
            },
        },
        {
            "$replaceRoot": {"newRoot": "$study"},
        },
    ]

    return strip_oid_recursive(
        {
            "count": list(
                mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(
                    [*aggregation, {"$count": "count"}]
                )
            )[0]["count"],
            "results": [
                doc
                for doc in mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(
                    [
                        *aggregation,
                        {"$skip": pagination.offset},
                        {"$limit": pagination.limit},
                    ]
                )
            ],
        }
    )


@router.post(
    "/study/facet",
    tags=["study"],
    name="Get all values of an attribute",
)
async def facet_study(
    study_query: FacetQuery,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Get counts of values of a study attribute."""
    aggregation = [
        {
            "$match": conditions_to_mongo_filter(study_query.conditions),
        },
        {
            "$unwind": {"path": "$study"},
        },
        {
            "$group": {
                "_id": "$study.id",
                "study": {"$first": "$study"},
            },
        },
        {
            "$replaceRoot": {"newRoot": "$study"},
        },
        {
            "$sortByCount": f"${study_query.attribute}",
        },
    ]

    return strip_oid_recursive(
        {
            "facets": {
                facet_value_to_key(facet["_id"]): facet["count"]
                for facet in mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(aggregation)
            },
        }
    )


@router.get(
    "/study/{study_id}",
    tags=["study"],
)
async def get_study(
    study_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Get study by ID."""
    studies = list(mdb.study_transformed.find({"id": study_id}))
    if len(studies) == 0:
        raise HTTPException(status_code=404, detail="Study not found")

    return strip_oid_recursive(studies[0])


# @router.get("/study/{study_id}/image", tags=["study"])
# async def get_study_image(study_id: str, db: Session = Depends(get_db)):
#     image = crud.get_study_image(db, study_id)
#     if image is None:
#         raise HTTPException(status_code=404, detail="No image exists for this study")
#     return StreamingResponse(BytesIO(image), media_type="image/jpeg")


@router.post(
    "/omics_processing/facet",
    tags=["omics_processing"],
    name="Get all values of an attribute",
)
async def facet_omics_processing(
    omics_processing_query: FacetQuery,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Get all values of an attribute."""
    aggregation = [
        {
            "$match": conditions_to_mongo_filter(
                omics_processing_query.conditions,
                "omics_processing",
            ),
        },
    ]

    aggregation += [
        {
            "$sortByCount": f"${omics_processing_query.attribute}",
        },
    ]

    return strip_oid_recursive(
        {
            "facets": {
                facet_value_to_key(facet["_id"]): facet["count"]
                for facet in mdb[OMICS_PROCESSING_SEARCH_COLLECTION].aggregate(
                    aggregation
                )
            },
        }
    )


# @router.get(
#     "/data_object/{data_object_id}/download",
#     tags=["data_object"],
#     responses=login_required_responses,
# )
# async def download_data_object(
#     data_object_id: str,
#     user_agent: Optional[str] = Header(None),
#     x_forwarded_for: Optional[str] = Header(None),
#     db: Session = Depends(get_db),
#     user: models.User = Depends(login_required),
# ):
#     ip = (x_forwarded_for or "").split(",")[0].strip()
#     data_object = crud.get_data_object(db, data_object_id)
#     if data_object is None:
#         raise HTTPException(status_code=404, detail="DataObject not found")
#     url = data_object.url
#     if url is None:
#         raise HTTPException(status_code=404, detail="DataObject has no url reference")

#     file_download = schemas.FileDownloadCreate(
#         ip=ip,
#         user_agent=user_agent,
#         orcid=user.orcid,
#         data_object_id=data_object_id,
#     )
#     crud.create_file_download(db, file_download)
#     return RedirectResponse(url=url)


@router.post(
    "/data_object/workflow_summary",
    tags=["data_object"],
    name="Aggregate data objects by workflow",
)
def data_object_aggregation(
    data_object_query: DataObjectQuerySchema = DataObjectQuerySchema(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """Aggregate data objects by workflow."""
    aggregation = [
        {
            "$match": conditions_to_mongo_filter(data_object_query.conditions),
        },
        {
            "$unwind": {"path": "$data_object"},
        },
        {
            "$group": {
                "_id": "$data_object.id",
                "data_object": {"$first": "$data_object"},
            },
        },
        {
            "$replaceRoot": {"newRoot": "$data_object"},
        },
        {
            "$set": {
                "combined_type": {
                    "data_object_type": "$data_object_type",
                    "activity_type": "$activity_type",
                },
            },
        },
        {
            "$sortByCount": "$combined_type",
        },
    ]

    result: Dict[str, Any] = dict()
    for facet in mdb[BIOSAMPLE_SEARCH_COLLECTION].aggregate(aggregation):
        if "data_object_type" not in facet["_id"]:
            # We are not considering data_objects without a data_object_type
            continue
        if result.get(facet["_id"]["activity_type"]) is None:
            result[facet["_id"]["activity_type"]] = {"count": 0, "file_types": dict()}
        result[facet["_id"]["activity_type"]]["file_types"][
            facet["_id"]["data_object_type"]
        ] = facet["count"]
        result[facet["_id"]["activity_type"]]["count"] += facet["count"]

    return strip_oid_recursive(result)


# @router.get(
#     "/principal_investigator/{principal_investigator_id}",
#     tags=["principal_investigator"]
# )
# async def get_pi_image(principal_investigator_id: UUID, db: Session = Depends(get_db)):
#     image = crud.get_pi_image(db, principal_investigator_id)
#     if image is None:
#         raise HTTPException(status_code=404, detail="Principal investigator  not found")

#     return StreamingResponse(BytesIO(image), media_type="image/jpeg")


# from fastapi import Header
# from fastapi.responses import JSONResponse, Response
# from uuid import UUID
# from sqlalchemy.orm import Session
# from nmdc_server.auth import login_required_responses, login_required, admin_required
# from nmdc_server import crud, jobs, query, models, schemas, schemas_submission
# from nmdc_server.bulk_download_schema import BulkDownload, BulkDownloadCreate
# from nmdc_server.database import get_db
# from nmdc_server.models import User, SubmissionMetadata

# @router.post(
#     "/jobs/ping",
#     tags=["jobs"],
#     responses=login_required_responses,
# )
# async def ping_celery(user: models.User = Depends(admin_required)) -> bool:
#     try:
#         return jobs.ping.delay().wait(timeout=0.5)
#     except TimeoutError:
#         return False


# @router.post(
#     "/jobs/ingest",
#     tags=["jobs"],
#     responses=login_required_responses,
# )
# async def run_ingest(
#     user: models.User = Depends(admin_required),
#     params: schemas.IngestArgumentSchema = schemas.IngestArgumentSchema(),
#     db: Session = Depends(get_db),
# ):
#     lock = db.query(IngestLock).first()
#     if lock:
#         raise HTTPException(
#             status_code=409,
#             detail=f"An ingest started at {lock.started} is already in progress",
#         )
#     jobs.ingest.delay(
#         function_limit=params.function_limit,
#         skip_annotation=params.skip_annotation
#     )
#     return ""


# @router.post(
#     "/bulk_download",
#     tags=["download"],
#     response_model=BulkDownload,
#     responses=login_required_responses,
#     status_code=201,
# )
# async def create_bulk_download(
#     user_agent: Optional[str] = Header(None),
#     x_forwarded_for: Optional[str] = Header(None),
#     query: query.BiosampleQuerySchema = query.BiosampleQuerySchema(),
#     db: Session = Depends(get_db),
#     user: models.User = Depends(login_required),
# ):
#     ip = (x_forwarded_for or "").split(",")[0].strip()
#     bulk_download = crud.create_bulk_download(
#         db,
#         BulkDownloadCreate(
#             ip=ip,
#             user_agent=user_agent,
#             orcid=user.orcid,
#             conditions=query.conditions,
#             filter=query.data_object_filter,
#         ),
#     )
#     if bulk_download is None:
#         return JSONResponse(status_code=400, content={"error": "no files matched the filter"})
#     return bulk_download


# @router.post(
#     "/bulk_download/summary",
#     tags=["download"],
#     response_model=query.DataObjectAggregation,
# )
# async def get_data_object_aggregation(
#     query: query.DataObjectQuerySchema = query.DataObjectQuerySchema(),
#     db: Session = Depends(get_db),
# ):
#     return query.aggregate(db)


# @router.get(
#     "/bulk_download/{bulk_download_id}",
#     tags=["download"],
#     responses=login_required_responses,
# )
# async def download_zip_file(
#     bulk_download_id: UUID,
#     db: Session = Depends(get_db),
#     user: models.User = Depends(login_required),
# ):
#     table = crud.get_zip_download(db, bulk_download_id)
#     return Response(
#         content=table,
#         headers={
#             "X-Archive-Files": "zip",
#             "Content-Disposition": "attachment; filename=archive.zip",
#         },
#     )


@router.get(
    "/metadata_submission",
    tags=["metadata_submission"],
    # responses=login_required_responses,
    response_model=MetadataSubmissionResponse,
)
async def list_submissions(
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
    pagination: Pagination = Depends(),
):
    # query = db.query(SubmissionMetadata)
    # try:
    #     await admin_required(user)
    # except HTTPException:
    #     query = query.join(User).filter(User.orcid == user.orcid)
    # return pagination.response(query)
    return [user]


# @router.get(
#     "/metadata_submission/{id}",
#     tags=["metadata_submission"],
#     responses=login_required_responses,
#     response_model=schemas_submission.SubmissionMetadataSchema,
# )
# async def get_submission(
#     id: str,
#     db: Session = Depends(get_db),
#     user: models.User = Depends(login_required),
# ):
#     submission = db.query(SubmissionMetadata).get(id)
#     if submission is None:
#         raise HTTPException(status_code=404, detail="Submission not found")
#     if submission.author.orcid != user.orcid:
#         await admin_required(user)
#     return submission


# @router.patch(
#     "/metadata_submission/{id}",
#     tags=["metadata_submission"],
#     responses=login_required_responses,
#     response_model=schemas_submission.SubmissionMetadataSchema,
# )
# async def update_submission(
#     id: str,
#     body: schemas_submission.SubmissionMetadataSchemaCreate,
#     db: Session = Depends(get_db),
#     user: models.User = Depends(login_required),
# ):
#     submission = db.query(SubmissionMetadata).get(id)
#     body_dict = body.dict()
#     if submission is None:
#         raise HTTPException(status_code=404, detail="Submission not found")
#     if submission.author_orcid != user.orcid:
#         await admin_required(user)
#     submission.metadata_submission = body_dict["metadata_submission"]
#     if body_dict["status"]:
#         submission.status = body_dict["status"]
#     db.commit()
#     return submission


# @router.post(
#     "/metadata_submission",
#     tags=["metadata_submission"],
#     responses=login_required_responses,
#     response_model=schemas_submission.SubmissionMetadataSchema,
#     status_code=201,
# )
# async def submit_metadata(
#     body: schemas_submission.SubmissionMetadataSchemaCreate,
#     db: Session = Depends(get_db),
#     user: models.User = Depends(login_required),
# ):
#     submission = SubmissionMetadata(**body.dict(), author_orcid=user.orcid)
#     submission.author_id = user.id
#     db.add(submission)
#     db.commit()
#     return submission


# @router.get(
#     "/users", responses=login_required_responses, response_model=query.UserResponse, tags=["user"]
# )
# async def get_users(
#     db: Session = Depends(get_db),
#     user: models.User = Depends(admin_required),
#     pagination: Pagination = Depends(),
# ):
#     users = db.query(User)
#     return pagination.response(users)


# @router.post(
#     "/users/{id}", responses=login_required_responses, response_model=schemas.User, tags=["user"]
# )
# async def update_user(
#     id: UUID,
#     body: schemas.User,
#     db: Session = Depends(get_db),
#     current_user: models.User = Depends(admin_required),
# ):
#     if body.id != id:
#         raise HTTPException(status_code=400, detail="Invalid id")
#     return crud.update_user(db, body)
