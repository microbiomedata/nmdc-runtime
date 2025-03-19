from operator import itemgetter
from typing import List, Annotated

from fastapi import APIRouter, Depends, Form, Path, Query
from jinja2 import Environment, PackageLoader, select_autoescape
from nmdc_runtime.minter.config import typecodes
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pymongo.database import Database as MongoDatabase
from starlette.responses import HTMLResponse
from toolz import merge, assoc_in

from nmdc_schema.get_nmdc_view import ViewGetter
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    activity_collection_names,
    get_planned_process_collection_names,
    get_nonempty_nmdc_schema_collection_names,
)
from nmdc_runtime.api.endpoints.util import (
    find_resources,
    strip_oid,
    find_resources_spanning,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import (
    FindResponse,
    FindRequest,
    entity_attributes_to_index,
)
from nmdc_runtime.util import get_class_names_from_collection_spec

router = APIRouter()


@router.get(
    "/studies",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_studies(
    req: Annotated[FindRequest, Query()],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    The `GET /studies` endpoint is a general purpose way to retrieve NMDC studies based on parameters provided by the user.
    Studies can be filtered and sorted based on the applicable [Study attributes](https://microbiomedata.github.io/nmdc-schema/Study/).
    """
    return find_resources(req, mdb, "study_set")


@router.get(
    "/studies/{study_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_study_by_id(
    study_id: Annotated[
        str,
        Path(
            title="Study ID",
            description="The `id` of the `Study` you want to find.\n\n_Example_: `nmdc:sty-11-abc123`",
            examples=["nmdc:sty-11-abc123"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    If the study identifier is known, a study can be retrieved directly using the GET /studies/{study_id} endpoint.
    \n Note that only one study can be retrieved at a time using this method.
    """
    return strip_oid(raise404_if_none(mdb["study_set"].find_one({"id": study_id})))


@router.get(
    "/biosamples",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_biosamples(
    req: Annotated[FindRequest, Query()],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    The GET /biosamples endpoint is a general purpose way to retrieve biosample metadata using user-provided filter and sort criteria.
    Please see the applicable [Biosample attributes](https://microbiomedata.github.io/nmdc-schema/Biosample/).
    """
    return find_resources(req, mdb, "biosample_set")


@router.get(
    "/biosamples/{sample_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_biosample_by_id(
    sample_id: Annotated[
        str,
        Path(
            title="Biosample ID",
            description="The `id` of the `Biosample` you want to find.\n\n_Example_: `nmdc:bsm-11-abc123`",
            examples=["nmdc:bsm-11-abc123"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    If the biosample identifier is known, a biosample can be retrieved directly using the GET /biosamples/{sample_id}.
    \n Note that only one biosample metadata record can be retrieved at a time using this method.
    """
    return strip_oid(raise404_if_none(mdb["biosample_set"].find_one({"id": sample_id})))


@router.get(
    "/data_objects",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_data_objects(
    req: Annotated[FindRequest, Query()],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    To retrieve metadata about NMDC data objects (such as files, records, or omics data) the GET /data_objects endpoint
    may be used along with various parameters. Please see the applicable [Data Object](https://microbiomedata.github.io/nmdc-schema/DataObject/)
    attributes.
    """
    return find_resources(req, mdb, "data_object_set")


def get_classname_from_typecode(doc_id: str) -> str:
    r"""
    Returns the name of the schema class of which an instance could have the specified `id`.

    >>> get_classname_from_typecode("nmdc:sty-11-r2h77870")
    'Study'
    """
    typecode = doc_id.split(":")[1].split("-")[0]
    class_map_data = typecodes()
    class_map = {
        entry["name"]: entry["schema_class"].split(":")[1] for entry in class_map_data
    }
    return class_map.get(typecode)


@router.get(
    "/data_objects/study/{study_id}",
    response_model_exclude_unset=True,
    #
    # Customize the name that Swagger UI displays for the API endpoint.
    #
    # Note: By default, FastAPI derives the name of the API endpoint from the name of the decorated function. Here, we
    #       are using a custom name that matches the derived one, except that the custom one ends with `(delayed)`.
    #
    # Note: Each word in the name will appear capitalized on Swagger UI.
    #
    name="Find data objects for study (delayed)",
    #
    # Customize the description that Swagger UI displays for the API endpoint.
    #
    # Note: By default, FastAPI derives the description of the API endpoint from the docstring of the decorated
    #       function. Here, we are using a custom description that was written for an audience of API consumers,
    #       as opposed to the derived description that was written for an audience of `nmdc-runtime` developers.
    #
    description=(
        "Gets all `DataObject`s related to all `Biosample`s related to the specified `Study`."
        "<br /><br />"  # newlines
        "**Note:** The data returned by this API endpoint can be up to 24 hours out of date "
        "with respect to the NMDC database. That's because the cache that underlies this API "
        "endpoint gets refreshed to match the NMDC database once every 24 hours."
    ),
)
def find_data_objects_for_study(
    study_id: Annotated[
        str,
        Path(
            title="Study ID",
            description="""The `id` of the `Study` having `Biosample`s with which you want to find
                        associated `DataObject`s.\n\n_Example_: `nmdc:sty-11-abc123`""",
            examples=["nmdc:sty-11-abc123"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """This API endpoint is used to retrieve data objects associated with
    all the biosamples associated with a given study. This endpoint makes
    use of the `alldocs` collection for its implementation.

    :param study_id: NMDC study id for which data objects are to be retrieved
    :param mdb: PyMongo connection, defaults to Depends(get_mongo_db)
    :return: List of dictionaries, each of which has a `biosample_id` entry
        and a `data_object_set` entry. The value of the `biosample_id` entry
        is the `Biosample`'s `id`. The value of the `data_object_set` entry
        is a list of the `DataObject`s associated with that `Biosample`.
    """
    biosample_data_objects = []
    study = raise404_if_none(
        mdb.study_set.find_one({"id": study_id}, ["id"]), detail="Study not found"
    )

    # Note: With nmdc-schema v10 (legacy schema), we used the field named `part_of` here.
    #       With nmdc-schema v11 (Berkeley schema), we use the field named `associated_studies` here.
    biosamples = mdb.biosample_set.find({"associated_studies": study["id"]}, ["id"])
    biosample_ids = [biosample["id"] for biosample in biosamples]

    # SchemaView interface to NMDC Schema
    nmdc_view = ViewGetter()
    nmdc_sv = nmdc_view.get_view()
    dg_descendants = nmdc_sv.class_descendants("DataGeneration")

    def collect_data_objects(doc_ids, collected_objects, unique_ids):
        """Helper function to collect data objects from `has_input` and `has_output` references."""
        for doc_id in doc_ids:
            if (
                get_classname_from_typecode(doc_id) == "DataObject"
                and doc_id not in unique_ids
            ):
                data_obj = mdb.data_object_set.find_one({"id": doc_id})
                if data_obj:
                    collected_objects.append(strip_oid(data_obj))
                    unique_ids.add(doc_id)

    # Another way in which DataObjects can be related to Biosamples is through the
    # `was_informed_by` key/slot. We need to link records from the `workflow_execution_set`
    # collection that are "informed" by the same DataGeneration records that created
    # the outputs above. Then we need to get additional DataObject records that are
    # created by this linkage.
    def process_informed_by_docs(doc, collected_objects, unique_ids):
        """Process documents linked by `was_informed_by` and collect relevant data objects."""
        informed_by_docs = mdb.workflow_execution_set.find(
            {"was_informed_by": doc["id"]}
        )
        for informed_doc in informed_by_docs:
            collect_data_objects(
                informed_doc.get("has_input", []), collected_objects, unique_ids
            )
            collect_data_objects(
                informed_doc.get("has_output", []), collected_objects, unique_ids
            )

    biosample_data_objects = []

    for biosample_id in biosample_ids:
        current_ids = [biosample_id]
        collected_data_objects = []
        unique_ids = set()

        # Iterate over records in the `alldocs` collection. Look for
        # records that have the given biosample_id as value on the
        # `has_input` key/slot. The retrieved documents might also have a
        # `has_output` key/slot associated with them. Get the value of the
        # `has_output` key and check if it's type is `nmdc:DataObject`. If
        # it's not, repeat the process till it is.
        while current_ids:
            new_current_ids = []
            for current_id in current_ids:
                # Query to find all documents with current_id as the value on
                # `has_input` slot
                for doc in mdb.alldocs.find({"has_input": current_id}):
                    has_output = doc.get("has_output", [])

                    # Process `DataGeneration` type documents linked by `was_informed_by`
                    if not has_output and any(
                        t in dg_descendants for t in doc.get("_type_and_ancestors", [])
                    ):
                        process_informed_by_docs(
                            doc, collected_data_objects, unique_ids
                        )
                        continue

                    collect_data_objects(has_output, collected_data_objects, unique_ids)
                    new_current_ids.extend(
                        op
                        for op in has_output
                        if get_classname_from_typecode(op) != "DataObject"
                    )

                    if any(
                        t in dg_descendants for t in doc.get("_type_and_ancestors", [])
                    ):
                        process_informed_by_docs(
                            doc, collected_data_objects, unique_ids
                        )

            current_ids = new_current_ids

        if collected_data_objects:
            result = {
                "biosample_id": biosample_id,
                "data_objects": collected_data_objects,
            }
            biosample_data_objects.append(result)

    return biosample_data_objects


@router.get(
    "/data_objects/{data_object_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_data_object_by_id(
    data_object_id: Annotated[
        str,
        Path(
            title="DataObject ID",
            description="The `id` of the `DataObject` you want to find.\n\n_Example_: `nmdc:dobj-11-abc123`",
            examples=["nmdc:dobj-11-abc123"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    If the data object identifier is known, the metadata can be retrieved using the GET /data_objects/{data_object_id} endpoint.
    \n Note that only one data object metadata record may be retrieved at a time using this method.
    """
    return strip_oid(
        raise404_if_none(mdb["data_object_set"].find_one({"id": data_object_id}))
    )


@router.get(
    "/planned_processes",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
def find_planned_processes(
    req: Annotated[FindRequest, Query()],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    The GET /planned_processes endpoint is a general way to fetch metadata about various planned processes (e.g.
    workflow execution, material processing, etc.). Any "slot" (a.k.a. attribute) for
    [`PlannedProcess`](https://w3id.org/nmdc/PlannedProcess) may be used in the filter
    and sort parameters, including attributes of subclasses of *PlannedProcess*.

    For example, attributes used in subclasses such as [`Extraction`](https://w3id.org/nmdc/Extraction)
    (subclass of *PlannedProcess*), can be used as input criteria for the filter and sort parameters of this endpoint.
    """
    return find_resources_spanning(
        req,
        mdb,
        get_planned_process_collection_names()
        & get_nonempty_nmdc_schema_collection_names(mdb),
    )


@router.get(
    "/planned_processes/{planned_process_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def find_planned_process_by_id(
    planned_process_id: Annotated[
        str,
        Path(
            title="PlannedProcess ID",
            description="The `id` of the document that represents an instance of "
            "the `PlannedProcess` class or any of its subclasses",
            example=r"nmdc:wfmag-11-00jn7876.1",
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    Returns the document that has the specified `id` and represents an instance of the `PlannedProcess` class
    or any of its subclasses. If no such document exists, returns an HTTP 404 response.
    """
    doc = None

    # Note: We exclude empty collections as a performance optimization
    #       (we already know they don't contain the document).
    collection_names = (
        get_planned_process_collection_names()
        & get_nonempty_nmdc_schema_collection_names(mdb)
    )

    # For each collection, search it for a document having the specified `id`.
    for name in collection_names:
        doc = mdb[name].find_one({"id": planned_process_id})
        if doc is not None:
            return strip_oid(doc)

    # Note: If execution gets to this point, it means we didn't find the document.
    return raise404_if_none(doc)


@router.get(
    "/related_objects/workflow_execution/{workflow_execution_id}",
    response_model_exclude_unset=True,
    name="Find related objects for workflow execution",
    description=(
        "Finds `DataObject`s, `Biosample`s, `Study`s, and other `WorkflowExecution`s "
        "related to the specified `WorkflowExecution`."
        "<br /><br />"  # newlines
        "This endpoint retrieves all DataObjects that are inputs to or outputs from the "
        "workflow execution, related workflow executions, and walks up the data provenance "
        "chain to find related biosamples and studies through DataGeneration activities."
    ),
)
def find_related_objects_for_workflow_execution(
    workflow_execution_id: Annotated[
        str,
        Path(
            title="Workflow Execution ID",
            description="""The `id` of the `WorkflowExecution` for which you want to find
                        related objects.\n\n_Example_: `nmdc:wfmag-11-abc123`""",
            examples=["nmdc:wfmag-11-abc123"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """This API endpoint retrieves all objects related to a workflow execution,
    including DataObjects that are inputs or outputs, other WorkflowExecution
    instances that are part of the same pipeline, and related Biosamples and Studies.

    :param workflow_execution_id: NMDC workflow execution id for which related objects are to be retrieved
    :param mdb: PyMongo connection, defaults to Depends(get_mongo_db)
    :return: Dictionary with data_objects, related_workflow_executions, biosamples, and studies lists
    """
    # Get the workflow execution
    workflow_execution = raise404_if_none(
        mdb.workflow_execution_set.find_one({"id": workflow_execution_id}),
        detail="Workflow execution not found",
    )

    # Initialize collections for related objects
    data_objects = []
    related_workflow_executions = []
    biosamples = []
    studies = []

    unique_data_object_ids = set()
    unique_workflow_execution_ids = set()
    unique_biosample_ids = set()
    unique_study_ids = set()

    # Add the requested workflow execution to the set of unique IDs
    unique_workflow_execution_ids.add(workflow_execution_id)

    # SchemaView interface to NMDC Schema
    nmdc_view = ViewGetter()
    nmdc_sv = nmdc_view.get_view()
    dg_descendants = nmdc_sv.class_descendants("DataGeneration")

    # Function to add data objects to the results
    def add_data_object(doc_id):
        if (
            get_classname_from_typecode(doc_id) == "DataObject"
            and doc_id not in unique_data_object_ids
        ):
            data_obj = mdb.data_object_set.find_one({"id": doc_id})
            if data_obj:
                data_objects.append(strip_oid(data_obj))
                unique_data_object_ids.add(doc_id)
                return True
        return False

    # Function to add related workflow executions
    def add_workflow_execution(wf):
        if wf["id"] not in unique_workflow_execution_ids:
            wf_stripped = strip_oid(wf)
            related_workflow_executions.append(wf_stripped)
            unique_workflow_execution_ids.add(wf["id"])

            # Add data objects from this related workflow
            for doc_id in wf.get("has_input", []) + wf.get("has_output", []):
                add_data_object(doc_id)

    # Function to add biosamples to the results
    def add_biosample(biosample_id):
        if biosample_id not in unique_biosample_ids:
            biosample = mdb.biosample_set.find_one({"id": biosample_id})
            if biosample:
                biosamples.append(strip_oid(biosample))
                unique_biosample_ids.add(biosample_id)

                # Add associated studies
                for study_id in biosample.get("associated_studies", []):
                    add_study(study_id)
                return True
        return False

    # Function to add studies to the results
    def add_study(study_id):
        if study_id not in unique_study_ids:
            study = mdb.study_set.find_one({"id": study_id})
            if study:
                studies.append(strip_oid(study))
                unique_study_ids.add(study_id)
                return True
        return False

    # Function to recursively find biosamples by walking up the chain
    def find_biosamples_recursively(start_id):
        # Set to track IDs we've already processed to avoid cycles
        processed_ids = set()

        def process_id(current_id):
            if current_id in processed_ids:
                return

            processed_ids.add(current_id)

            # If it's a biosample, add it directly
            if get_classname_from_typecode(current_id) == "Biosample":
                add_biosample(current_id)
                return

            # Find the document with this ID to see what it is
            current_doc = mdb.alldocs.find_one({"id": current_id})
            if current_doc:
                # Check if this document has inputs - if so, process them
                for input_id in current_doc.get("has_input", []):
                    if input_id not in processed_ids:
                        process_id(input_id)

            # Also find documents that have this ID as an output
            # This is the key to walking backward through the chain
            for doc in mdb.alldocs.find({"has_output": current_id}):
                # Process all inputs of this document
                for input_id in doc.get("has_input", []):
                    if input_id not in processed_ids:
                        process_id(input_id)

        # Start the recursive search
        process_id(start_id)

    # Collect input and output data objects directly from the workflow
    input_ids = workflow_execution.get("has_input", [])
    output_ids = workflow_execution.get("has_output", [])

    # Add data objects from input and output
    for doc_id in input_ids + output_ids:
        add_data_object(doc_id)

    # Find workflows that use outputs of this workflow as inputs
    for output_id in output_ids:
        related_wfs = mdb.workflow_execution_set.find({"has_input": output_id})
        for wf in related_wfs:
            add_workflow_execution(wf)

    # Find workflows whose outputs are used as inputs to this workflow
    for input_id in input_ids:
        related_wfs = mdb.workflow_execution_set.find({"has_output": input_id})
        for wf in related_wfs:
            add_workflow_execution(wf)

    # Find workflows that share the same "was_informed_by" relationship
    if "was_informed_by" in workflow_execution:
        informed_by = workflow_execution["was_informed_by"]
        related_wfs = mdb.workflow_execution_set.find({"was_informed_by": informed_by})
        for wf in related_wfs:
            if wf["id"] != workflow_execution_id:
                add_workflow_execution(wf)

        # Look for the DataGeneration instance in alldocs - this is the direct path to biosamples
        dg_doc = mdb.alldocs.find_one({"id": informed_by})
        if dg_doc and any(
            t in dg_descendants for t in dg_doc.get("_type_and_ancestors", [])
        ):
            # Get biosamples from has_input by recursively walking up the chain
            for input_id in dg_doc.get("has_input", []):
                find_biosamples_recursively(input_id)

            # Get studies directly associated with the DataGeneration
            for study_id in dg_doc.get("associated_studies", []):
                add_study(study_id)

            # If the data generation has no associated studies but is linked to biosamples,
            # we should still try to get the studies from the biosamples
            if not dg_doc.get("associated_studies") and len(biosamples) > 0:
                for bs in biosamples:
                    for study_id in bs.get("associated_studies", []):
                        add_study(study_id)

    # For all data objects we collected, check if they have a was_generated_by reference
    # This is a supplementary path to find more relationships
    for data_obj in data_objects:
        if "was_generated_by" in data_obj:
            gen_id = data_obj["was_generated_by"]
            dg_doc = mdb.alldocs.find_one({"id": gen_id})

            if dg_doc and any(
                t in dg_descendants for t in dg_doc.get("_type_and_ancestors", [])
            ):
                # Get studies directly associated with the DataGeneration
                for study_id in dg_doc.get("associated_studies", []):
                    add_study(study_id)

    # Return structured response
    response = {
        "workflow_execution_id": workflow_execution_id,
        "data_objects": data_objects,
        "related_workflow_executions": related_workflow_executions,
        "biosamples": biosamples,
        "studies": studies,
    }

    return response


jinja_env = Environment(
    loader=PackageLoader("nmdc_runtime"), autoescape=select_autoescape()
)


def attr_index_sort_key(attr):
    return "_" if attr == "id" else attr


def documentation_links(jsonschema_dict, collection_names) -> dict:
    """This function constructs a hierarchical catalog of (links to) schema classes and their slots.

    The returned dictionary `doc_links` is used as input to the Jinja template `nmdc_runtime/templates/search.html`
    in order to support user experience for `GET /search`.
    """

    # Note: All documentation URLs generated within this function will begin with this.
    base_url = r"https://w3id.org/nmdc"

    # Initialize dictionary in which to associate key/value pairs via the following for loop.
    doc_links = {}

    for collection_name in collection_names:
        # Since a given collection can be associated with multiple classes, the `doc_links` dictionary
        # will have a _list_ of values for each collection.
        class_descriptors = []

        # If the collection name is one that the `search.html` page has a dedicated section for,
        # give it a top-level key; otherwise, nest it under `activity_set`.
        key_hierarchy: List[str] = ["activity_set", collection_name]
        if collection_name in ("biosample_set", "study_set", "data_object_set"):
            key_hierarchy = [collection_name]

        # Process the name of each class that the schema associates with this collection.
        collection_spec = jsonschema_dict["$defs"]["Database"]["properties"][
            collection_name
        ]
        class_names = get_class_names_from_collection_spec(collection_spec)
        for idx, class_name in enumerate(class_names):
            # Make a list of dictionaries, each of which describes one attribute of this class.
            entity_attrs = list(jsonschema_dict["$defs"][class_name]["properties"])
            entity_attr_descriptors = [
                {"url": f"{base_url}/{attr_name}", "attr_name": attr_name}
                for attr_name in entity_attrs
            ]

            # Make a dictionary describing this class.
            class_descriptor = {
                "collection_name": collection_name,
                "entity_url": f"{base_url}/{class_name}",
                "entity_name": class_name,
                "entity_attrs": sorted(
                    entity_attr_descriptors, key=itemgetter("attr_name")
                ),
            }

            # Add that descriptor to this collection's list of class descriptors.
            class_descriptors.append(class_descriptor)

        # Add a key/value pair describing this collection to the `doc_links` dictionary.
        # Reference: https://toolz.readthedocs.io/en/latest/api.html#toolz.dicttoolz.assoc_in
        doc_links = assoc_in(doc_links, keys=key_hierarchy, value=class_descriptors)

    return doc_links


@router.get("/search", response_class=HTMLResponse, include_in_schema=False)
def search_page(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    template = jinja_env.get_template("search.html")
    indexed_entity_attributes = merge(
        {n: {"id"} for n in activity_collection_names(mdb)},
        {
            coll: sorted(attrs | {"id"}, key=attr_index_sort_key)
            for coll, attrs in entity_attributes_to_index.items()
        },
    )
    doc_links = documentation_links(
        get_nmdc_jsonschema_dict(),
        (
            list(activity_collection_names(mdb))
            + ["biosample_set", "study_set", "data_object_set"]
        ),
    )
    html_content = template.render(
        activity_collection_names=sorted(activity_collection_names(mdb)),
        indexed_entity_attributes=indexed_entity_attributes,
        doc_links=doc_links,
    )
    return HTMLResponse(content=html_content, status_code=200)
