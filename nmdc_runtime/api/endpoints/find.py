import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from nmdc_runtime.mongo_util import (
    get_runtime_mdb,
    RuntimeAsyncMongoDatabase,
)

from nmdc_schema.get_nmdc_view import ViewGetter
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import (
    get_planned_process_collection_names,
    get_nonempty_nmdc_schema_collection_names,
)
from nmdc_runtime.api.endpoints.nmdcschema import get_linked_instances
from nmdc_runtime.api.endpoints.util import (
    find_resources,
    strip_oid,
    find_resources_spanning,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import (
    FindResponse,
    FindRequest,
)


router = APIRouter()


@router.get(
    "/studies",
    response_model=FindResponse,
    response_model_exclude_unset=True,
)
async def find_studies(
    req: Annotated[FindRequest, Query()],
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    """
    A general-purpose way to retrieve NMDC studies based on parameters provided by the user.
    Studies can be filtered and sorted based on the applicable [nmdc:Study](https://w3id.org/nmdc/Study) attributes.
    """
    return await find_resources(req, mdb, "study_set")


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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    """
    To retrieve metadata about NMDC data objects (such as files, records, or omics data) the GET /data_objects endpoint
    may be used along with various parameters. Please see the applicable [Data Object](https://microbiomedata.github.io/nmdc-schema/DataObject/)
    attributes.
    """
    return find_resources(req, mdb, "data_object_set")


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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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

    # Respond with an error if the specified `Study` does not exist.
    # Note: We project only the `_id` field, to minimize data transfer.
    raise404_if_none(
        mdb["study_set"].find_one({"id": study_id}, projection={"_id": 1}),
        detail="Study not found",
    )

    # Use the `get_linked_instances` function—which is the function that
    # underlies the `/nmdcschema/linked_instances` API endpoint—to get all
    # the `Biosample`s that are downstream of the specified `Study`.
    #
    # Note: The `get_linked_instances` function requires that a `max_page_size`
    #       integer argument be passed in. In our case, we want to get _all_ of
    #       the instances. Python has no "infinity" integer; and, even if it did,
    #       if we were to specify too large of an integer, we'd get this error:
    #       > "OverflowError: MongoDB can only handle up to 8-byte ints"
    #       So, as a workaround, we pass in a number that is large enough that we
    #       think it will account for all cases in practice (e.g., a study having
    #       a trillion biosamples or a trillion data objects).
    #
    #       TODO: Update the `get_linked_instances` function to optionally impose _no_ limit.
    #
    large_max_page_size: int = 1_000_000_000_000
    linked_biosamples_result: dict = get_linked_instances(
        ids=[study_id],
        types=["nmdc:Biosample"],
        hydrate=False,  # we'll only use their `id` values
        page_token=None,
        max_page_size=large_max_page_size,
        mdb=mdb,
    )
    biosample_ids = [d["id"] for d in linked_biosamples_result.get("resources", [])]
    logging.debug(f"Found {len(biosample_ids)} Biosamples for Study {study_id}")

    # Get all the `DataObject`s that are downstream from any of those `Biosample`s.
    data_objects_by_biosample_id = {}
    linked_data_objects_result: dict = get_linked_instances(
        ids=biosample_ids,
        types=["nmdc:DataObject"],
        hydrate=True,  # we want the full `DataObject` documents
        page_token=None,
        max_page_size=large_max_page_size,
        mdb=mdb,
    )
    for data_object in linked_data_objects_result.get("resources", []):
        upstream_biosample_id = data_object["_downstream_of"][0]
        if upstream_biosample_id not in data_objects_by_biosample_id.keys():
            data_objects_by_biosample_id[upstream_biosample_id] = []

        # Strip away the metadata fields injected by `get_linked_instances()`.
        data_object.pop("_upstream_of", None)
        data_object.pop("_downstream_of", None)
        data_objects_by_biosample_id[upstream_biosample_id].append(data_object)

    # Convert the `data_objects_by_biosample_id` dictionary into a list of dicts;
    # i.e., into the format returned by the initial version of this API endpoint,
    # which did not use the `get_linked_instances` function under the hood.
    for biosample_id, data_objects in data_objects_by_biosample_id.items():
        biosample_data_objects.append(
            {
                "biosample_id": biosample_id,
                "data_objects": data_objects,
            }
        )
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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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
            examples=[r"nmdc:wfmag-11-00jn7876.1"],
        ),
    ],
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
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
    "/workflow_executions/{workflow_execution_id}/related_resources",
    response_model_exclude_unset=True,
    name="Find resources related to the specified WorkflowExecution",
    description=(
        "Finds `DataObject`s, `Biosample`s, `Study`s, and other `WorkflowExecution`s "
        "related to the specified `WorkflowExecution`."
        "<br /><br />"  # newlines
        "This endpoint returns a JSON object that contains "
        "(a) the specified `WorkflowExecution`, "
        "(b) all the `DataObject`s that are inputs to — or outputs from — the specified `WorkflowExecution`, "
        "(c) all the `Biosample`s that were inputs to those `DataGeneration`s, "
        "(d) all the `Study`s with which those `Biosample`s are associated, and "
        "(e) all the other `WorkflowExecution`s that are part of the same processing pipeline "
        "as the specified `WorkflowExecution`."
        "<br /><br />"  # newlines
        "**Note:** The data returned by this API endpoint can be up to 24 hours out of date "
        "with respect to the NMDC database. That's because the cache that underlies this API "
        "endpoint gets refreshed to match the NMDC database once every 24 hours."
    ),
)
def find_related_objects_for_workflow_execution(
    workflow_execution_id: Annotated[
        str,
        Path(
            title="Workflow Execution ID",
            description=(
                "The `id` of the `WorkflowExecution` to which you want to find related resources."
                "\n\n"
                "_Example_: `nmdc:wfmgan-11-wdx72h27.1`"
            ),
            examples=["nmdc:wfmgan-11-wdx72h27.1"],
        ),
    ],
    mdb: RuntimeAsyncMongoDatabase = Depends(get_runtime_mdb),
):
    """This API endpoint retrieves resources related to the specified WorkflowExecution,
    including DataObjects that are inputs to — or outputs from — it, other WorkflowExecution
    instances that are part of the same pipeline, and related Biosamples and Studies.

    :param workflow_execution_id: id of workflow_execution_set instance for which related objects are to be retrieved
    :param mdb: A PyMongo`AsyncMongoDatabase` instance that can be used to access the MongoDB database
    :return: Dictionary with data_objects, related_workflow_executions, biosamples, and studies lists
    """
    # Get the specified `WorkflowExecution` document from the database.
    workflow_execution = raise404_if_none(
        mdb.workflow_execution_set.find_one({"id": workflow_execution_id}),
        detail="Workflow execution not found",
    )

    # Create empty lists that will contain the related documents we find.
    data_objects = []
    related_workflow_executions = []
    biosamples = []
    studies = []

    # Create empty sets that we'll use to avoid processing a given document multiple times.
    unique_data_object_ids = set()
    unique_workflow_execution_ids = set()
    unique_biosample_ids = set()
    unique_study_ids = set()

    # Add the ID of the specified `WorkflowExecution` document, to the set of unique `WorkflowExecution` IDs.
    unique_workflow_execution_ids.add(workflow_execution_id)

    # Get a `SchemaView` that is bound to the NMDC schema.
    nmdc_view = ViewGetter()
    nmdc_sv = nmdc_view.get_view()
    dg_descendants = [
        (f"nmdc:{t}" if ":" not in t else t)
        for t in nmdc_sv.class_descendants("DataGeneration")
    ]

    def add_data_object(doc_id: str) -> bool:
        r"""
        Helper function that adds the `DataObject` having the specified `id`
        to our list of `DataObjects`, if it isn't already in there.
        """
        # Check if this is a DataObject by looking at the document's type directly
        doc = mdb.alldocs.find_one({"id": doc_id}, {"type": 1})
        if (
            doc
            and doc.get("type") == "nmdc:DataObject"
            and doc_id not in unique_data_object_ids
        ):
            data_obj = mdb.data_object_set.find_one({"id": doc_id})
            if data_obj:
                data_objects.append(strip_oid(data_obj))
                unique_data_object_ids.add(doc_id)
                return True
        return False

    def add_workflow_execution(wfe: dict) -> None:
        r"""
        Helper function that adds the specified `WorkflowExecution`
        to our list of `WorkflowExecution`s, if it isn't already in there;
        and adds its related `DataObjects` to our list of `DataObject`s.
        """
        if wfe["id"] not in unique_workflow_execution_ids:
            related_workflow_executions.append(strip_oid(wfe))
            unique_workflow_execution_ids.add(wfe["id"])

            # Add data objects related to this workflow execution.
            ids_of_inputs = wfe.get("has_input", [])
            ids_of_outputs = wfe.get("has_output", [])
            for doc_id in ids_of_inputs + ids_of_outputs:
                add_data_object(doc_id)

    def add_biosample(biosample_id: str) -> bool:
        r"""
        Helper function that adds the specified `Biosample`
        to our list of `Biosample`s, if it isn't already in there;
        and adds its related `Study`s to our list of `Study`s.
        """
        if biosample_id not in unique_biosample_ids:
            biosample = mdb.biosample_set.find_one({"id": biosample_id})
            if biosample:
                biosamples.append(strip_oid(biosample))
                unique_biosample_ids.add(biosample_id)

                # Add studies related to this biosample.
                for study_id in biosample.get("associated_studies", []):
                    add_study(study_id)
                return True
        return False

    def add_study(study_id: str) -> bool:
        r"""
        Helper function that adds the specified `Study`
        to our list of `Study`s, if it isn't already in there.
        """
        if study_id not in unique_study_ids:
            study = mdb.study_set.find_one({"id": study_id})
            if study:
                studies.append(strip_oid(study))
                unique_study_ids.add(study_id)
                return True
        return False

    def find_biosamples_recursively(start_id: str) -> None:
        r"""
        Recursive helper function that traverses the database in search of relevant `Biosample`s.

        This function searches for biosamples starting from the "input" to a DataGeneration record by
        traversing the data provenance graph – which is the bipartite graph formed by the
        `has_input` / `has_output` relationships in the schema. It uses the ids asserted on
        `has_input` and `has_output` slots on documents in the `alldocs` collection to tie related documents
        in the chain together.

        Note: The function uses an internal nested recursive function (`process_id()`) to avoid cycles
        in the graph and tracks processed IDs to prevent infinite recursion.

        :param start_id: The ID of the document to start the search from. This will typically
            be the input to a `DataGeneration` record, which may be a `Biosample` directly or a
            `ProcessedSample`.
        """
        # Create an empty set we can use to track the `id`s of documents we've already processed,
        # in order to avoid processing the same documents multiple times (i.e. cycling in the graph).
        processed_ids = set()

        def process_id(current_id):
            r"""
            Recursive helper function that processes a single document ID and follows
            connections to discover related biosamples.

            This function:
            1. Checks if the current ID is already processed to prevent cycles
            2. Directly adds the document if it's a `Biosample`
            3. For non-Biosample documents (type of `PlannedProcess`), it:
               - Processes input (`has_input`) IDs of the current document
               - Finds documents that have the current ID as output (`has_output`) and processes their inputs

            This recursive approach allows traversing the provenance graph in both directions.

            :param current_id: The ID of the document to process in this recursive step
            """
            if current_id in processed_ids:
                return

            processed_ids.add(current_id)

            # If it's a `Biosample`, i.e., "type" == "nmdc:Biosample"
            doc = mdb.alldocs.find_one({"id": current_id}, {"type": 1})
            if doc and doc.get("type") == "nmdc:Biosample":
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

    # Get the DataObject `id`s that are inputs (`has_input`) to and
    # outputs (`has_output`) from the user-specified WorkflowExecution.
    input_ids = workflow_execution.get("has_input", [])
    output_ids = workflow_execution.get("has_output", [])

    # Add those DataObjects to our list of DataObjects.
    for doc_id in input_ids + output_ids:
        add_data_object(doc_id)

    # Find WorkflowExecutions whose inputs are outputs of this WorkflowExecution.
    # Add those to our list of related WorkflowExecutions.
    for output_id in output_ids:
        related_wfes = mdb.workflow_execution_set.find({"has_input": output_id})
        for wfe in related_wfes:
            add_workflow_execution(wfe)

    # Find WorkflowExecutions whose outputs are inputs of this WorkflowExecution.
    # Add those, too, to our list of related WorkflowExecutions.
    for input_id in input_ids:
        related_wfes = mdb.workflow_execution_set.find({"has_output": input_id})
        for wfe in related_wfes:
            add_workflow_execution(wfe)

    # Find WorkflowExecutions whose `was_informed_by` list contains that of the user-specified WorkflowExecution.
    # Add those, too, to our list of related WorkflowExecutions.
    if "was_informed_by" in workflow_execution:
        was_informed_by = workflow_execution["was_informed_by"]

        # Note: We added this assertion in an attempt to facilitate debugging
        #       the system in the situation where a `WorkflowExecution` document
        #       has a `was_informed_by` field whose value is not a list (which
        #       would be a violation of NMDC schema 11.9.0).
        assert isinstance(was_informed_by, list), (
            "A WorkflowExecution's `was_informed_by` field contained "
            f"a {type(was_informed_by)} instead of a list."
        )

        # Get all WorkflowExecutions that were informed by any of the
        # things that informed the user-specified WorkflowExecution.
        related_wfes = mdb.workflow_execution_set.find(
            {"was_informed_by": {"$in": was_informed_by}}
        )
        for wfe in related_wfes:
            if wfe["id"] != workflow_execution_id:
                add_workflow_execution(wfe)

        # Get all `DataGeneration`s that informed the user-specified `WorkflowExecution`, then
        # get all `Biosample`s and `Study`s associated with each of those `DataGeneration`s.
        dg_docs = mdb.alldocs.find({"id": {"$in": was_informed_by}})
        for dg_doc in dg_docs:
            if any(t in dg_descendants for t in dg_doc.get("_type_and_ancestors", [])):
                # Get Biosamples from the DataGeneration's `has_input` field by recursively walking up the chain.
                # While we recursively walk up the chain, we'll add those Biosamples to our list of Biosamples.
                for input_id in dg_doc.get("has_input", []):
                    find_biosamples_recursively(input_id)

                # Get Studies associated with the DataGeneration,
                # and add them to our list of Studies.
                for study_id in dg_doc.get("associated_studies", []):
                    add_study(study_id)

                # If the DataGeneration has no associated Studies, but has related Biosamples,
                # add the Studies associated with those Biosamples to our list of Studies.
                if not dg_doc.get("associated_studies") and len(biosamples) > 0:
                    for bs in biosamples:
                        for study_id in bs.get("associated_studies", []):
                            add_study(study_id)

    # For all data objects we collected, check if they have a `was_generated_by` reference
    # This is a supplementary path to find more relationships
    for data_obj in data_objects:
        if "was_generated_by" in data_obj:
            gen_id = data_obj["was_generated_by"]
            dg_doc = mdb.alldocs.find_one({"id": gen_id})

            if dg_doc and any(
                t in dg_descendants for t in dg_doc.get("_type_and_ancestors", [])
            ):
                # Get Studies directly associated with the DataGeneration
                for study_id in dg_doc.get("associated_studies", []):
                    add_study(study_id)

    response = {
        "workflow_execution_id": workflow_execution_id,  # `WorkflowExecution` `id` provided by user
        "workflow_execution": strip_oid(
            workflow_execution
        ),  # the specified `WorkflowExecution`
        "data_objects": data_objects,  # related `DataObject`s
        "related_workflow_executions": related_workflow_executions,  # related `WorkflowExecution`s
        "biosamples": biosamples,  # related `Biosample`s
        "studies": studies,  # related `Study`s
    }

    return response
