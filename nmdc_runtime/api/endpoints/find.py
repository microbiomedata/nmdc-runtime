import csv
import logging
import re
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from io import StringIO
from typing import Annotated, Dict, List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query, Response, status
from fastapi.responses import StreamingResponse
from pymongo.database import Database as MongoDatabase

from nmdc_schema.get_nmdc_view import ViewGetter
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    get_planned_process_collection_names,
    get_nonempty_nmdc_schema_collection_names,
)
from nmdc_runtime.api.endpoints.lib.linked_instances import (
    drop_stale_temp_linked_instances_collections,
    gather_linked_instances,
    hydrated,
)
from nmdc_runtime.api.endpoints.users import is_admin
from nmdc_runtime.api.endpoints.util import (
    find_resources,
    strip_oid,
    find_resources_spanning,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.api.models.util import (
    FindResponse,
    FindRequest,
)
from nmdc_runtime.util import duration_logger


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


@router.get(
    "/admin/data_object_urls",
    status_code=status.HTTP_200_OK,
    name="Get Data Object URLs",
    description="(Admins only) Download a TSV-formatted report consisting of the distinct URLs of all `DataObject`s that are outputs of any `WorkflowExecution`s.",
    responses={
        status.HTTP_200_OK: {"description": "TSV file containing DataObject URLs"},
        status.HTTP_204_NO_CONTENT: {
            "description": "No DataObject URLs found meet the specified criteria"
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "User is not authorized to access this resource"
        },
    },
)
def get_data_object_report(
    user: User = Depends(get_current_active_user),
    mdb: MongoDatabase = Depends(get_mongo_db),
    prefix: str = Query(
        "",
        description="A prefix, if any, a URL must begin with in order to be included in the report",
        example="https://data.microbiomedata.org",
    ),
):
    if not is_admin(user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Runtime administrators can access this resource.",
        )

    # Get the URL of every `DataObject` that is the output of any `WorkflowExecution` and has a URL;
    # filtering out URLs that don't begin with the specified prefix (if one was specified).
    #
    # Note: We use an aggregation pipeline instead of `distinct()` because the latter has a
    #       16 MB limit on the size of the result set, whereas the former does not.
    #
    data_object_urls = set()
    wfe_output_id_docs = mdb.workflow_execution_set.aggregate(
        [
            # Split each element of each `has_output` array into its own document
            # whose `has_output` field consists of that element.
            # Example: {"has_output": "nmdc:dobj-00-0001"}, {"has_output": "nmdc:dobj-00-0002"}, etc.
            {"$unwind": "$has_output"},
            # Group the resulting documents by their common `has_output` value,
            # so we end up with one document per distinct `has_output` value.
            # Example: {"_id": "nmdc:dobj-00-0001"}, {"_id": "nmdc:dobj-00-0002"}, etc.
            {"$group": {"_id": "$has_output"}},
        ]
    )
    wfe_output_ids = [doc["_id"] for doc in wfe_output_id_docs]
    url_filter = {"$exists": True, "$type": "string"}
    if len(prefix) > 0:
        # Note: We use `re.escape()` to ensure that characters like "?", "+", etc., get compared
        #       verbatim, regardless of any special meaning they might have in regex.
        url_filter["$regex"] = f"^{re.escape(prefix)}"
    for data_object in mdb.data_object_set.find(
        filter={"id": {"$in": wfe_output_ids}, "url": url_filter},
        projection={"url": True, "_id": False},
    ):
        data_object_urls.add(data_object["url"])

    # If no such URLs were found (e.g. if the prefix was "foobar"), return an HTTP 204 response.
    if len(data_object_urls) == 0:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Build the report as an in-memory TSV "file" (buffer), with one URL per row.
    # Reference: https://docs.python.org/3/library/csv.html#csv.writer
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter="\t", lineterminator="\n")
    writer.writerows([[url] for url in sorted(data_object_urls)])

    # Reset the buffer's internal file pointer to the beginning of the buffer, so that,
    # when we stream the buffer's contents later, all of its contents are included.
    buffer.seek(0)

    # Stream the buffer's contents to the HTTP client as a downloadable TSV file.
    filename = "data-object-urls.tsv"
    response = StreamingResponse(
        buffer,
        media_type="text/tab-separated-values",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

    return response


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
        "Gets all `DataObject`s related to all `Biosample`s associated with the specified `Study`."
        "<br /><br />"  # newlines
        "**Note:** The data returned by this API endpoint can be up to 60 minutes out of date "
        "with respect to the NMDC database. That's because the cache that underlies this API "
        "endpoint gets refreshed to match the NMDC database once every 60 minutes."
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
    background_tasks: BackgroundTasks,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """This API endpoint is used to retrieve data objects associated with
    all the biosamples associated with a given study. This endpoint makes
    use of the `alldocs` collection for its implementation.

    :param study_id: NMDC study id for which data objects are to be retrieved
    :param mdb: PyMongo connection, defaults to Depends(get_mongo_db)
    :return: List of dictionaries, each of which has a `biosample_id` entry
        and a `data_objects` entry. The value of the `biosample_id` entry
        is the `Biosample`'s `id`. The value of the `data_objects` entry
        is a list of the `DataObject`s associated with that `Biosample`.
        If a given `DataObject` is related to multiple `Biosample`s,
        it will appear in _each_ of those `Biosample`s' lists.
    """

    # Add a background task to clean up obsolete "linked instances" collections from MongoDB,
    # given that this endpoint causes some to be generated. Note that, since this is a
    # background task, it will not block the processing of the incoming HTTP request.
    # Reference: https://fastapi.tiangolo.com/tutorial/background-tasks/
    background_tasks.add_task(drop_stale_temp_linked_instances_collections)

    biosample_data_objects = []

    # Respond with an error if the specified `Study` does not exist.
    # Note: We project only the `_id` field, to minimize data transfer.
    raise404_if_none(
        mdb["study_set"].find_one({"id": study_id}, projection={"_id": 1}),
        detail="Study not found",
    )

    # Get the IDs of all the `Biosample`s associated with the specified `Study`.
    # Note: Getting the IDs this way is faster than doing it via `get_linked_instances`.
    logging.debug(f"Finding Biosamples associated with Study '{study_id}'")
    biosample_ids = []
    for doc in mdb.get_collection("biosample_set").find(
        {"associated_studies": {"$in": [study_id]}}, {"id": 1, "_id": 0}
    ):
        biosample_ids.append(doc["id"])
    num_biosample_ids = len(biosample_ids)
    logging.debug(f"Found {num_biosample_ids} Biosamples.")

    # Divide the `Biosample` IDs into batches (returning early if there are no such IDs).
    #
    # Note: This is a performance optimization of the `gather_linked_instances` function usage below.
    #       In our (local) testing with the 5260 `Biosample`s associated with the `Study` whose ID
    #       is "nmdc:sty-11-34xj1150", we found that processing the IDs in batches took less time,
    #       in total, than processing them in a single batch.
    #
    num_ids_per_batch = 500  # this can be "tuned"
    biosample_id_batches = []
    if num_biosample_ids == 0:
        return biosample_data_objects  # no need to proceed further if there are no biosample IDs
    if num_biosample_ids <= num_ids_per_batch:
        biosample_id_batches = [biosample_ids]  # a single batch
    else:
        # Make batches of `Biosample` IDs, where each batch has `num_ids_per_batch` IDs
        # (except for the final batch, which may have fewer).
        for i in range(0, num_biosample_ids, num_ids_per_batch):
            biosample_ids_batch = biosample_ids[i : i + num_ids_per_batch]
            biosample_id_batches.append(biosample_ids_batch)
    logging.debug(
        f"Divided {num_biosample_ids} Biosample IDs into {len(biosample_id_batches)} batches: "
        + " + ".join([str(len(batch)) for batch in biosample_id_batches])
    )

    def process_batch_of_biosample_ids(
        biosample_id_batch: List[str],
    ) -> Dict[str, List[dict]]:
        """
        Helper function that processes one batch of `Biosample` IDs and returns a dictionary
        mapping each `Biosample` ID in the batch to a list of the `DataObject`s that are
        downstream of that `Biosample`.
        """

        # Use the `gather_linked_instances` and `hydrated` functions—which underlie the
        # `/nmdcschema/linked_instances` API endpoint—to get the `DataObject`s that are
        # downstream of the `Biosample`s whose IDs are in this batch.
        data_objects_by_biosample_id_in_batch: dict = {}
        temp_linked_instances_collection_name: str = gather_linked_instances(
            alldocs_collection=mdb.get_collection("alldocs"),
            ids=biosample_id_batch,
            types=["nmdc:DataObject"],
        )
        linked_data_objects_dehydrated = list(
            mdb.get_collection(temp_linked_instances_collection_name).find({})
        )
        linked_data_objects = hydrated(linked_data_objects_dehydrated, mdb)
        logging.debug(f"Found {len(linked_data_objects)} DataObjects in this branch.")

        # For each `DataObject`, strip away extra fields and add it to the result for this batch.
        for data_object in linked_data_objects:

            # Strip away the `_id` field injected by MongoDB.
            data_object.pop("_id", None)

            # Strip away the metadata fields injected by `gather_linked_instances()`.
            upstream_biosample_ids = data_object["_downstream_of"]  # preserve its value
            data_object.pop("_upstream_of", None)
            data_object.pop("_downstream_of", None)

            # Store the `DataObject` in the list keyed by the `id` of each `Biosample` that is
            # upstream of it, of which there may be multiple (meaning that the same `DataObject`
            # may appear multiple times in the API response, but in different lists).
            for upstream_biosample_id in upstream_biosample_ids:
                if (
                    upstream_biosample_id
                    not in data_objects_by_biosample_id_in_batch.keys()
                ):
                    data_objects_by_biosample_id_in_batch[upstream_biosample_id] = []
                data_objects_by_biosample_id_in_batch[upstream_biosample_id].append(
                    data_object
                )
        return data_objects_by_biosample_id_in_batch

    # Process all batches in parallel instead of serially (this is a performance optimization).
    with duration_logger(
        logging.debug,
        f"Finding DataObjects downstream of those {num_biosample_ids} Biosamples",
    ):
        data_objects_by_biosample_id = {}
        max_num_batches_in_parallel = min(
            8, len(biosample_id_batches)
        )  # this can be "tuned"
        with ThreadPoolExecutor(
            max_workers=max_num_batches_in_parallel
        ) as thread_pool_executor:
            future_to_biosample_id_batch_map: Dict[Future, list] = dict()
            for biosample_id_batch in biosample_id_batches:
                future: Future = thread_pool_executor.submit(
                    process_batch_of_biosample_ids, biosample_id_batch
                )

                # Keep track of which "future" corresponds to this batch of `Biosample` IDs.
                future_to_biosample_id_batch_map[future] = biosample_id_batch

            # Whenever the thread pool executor finishes processing a given batch (at which point,
            # the "future" corresponding to that batch will be completed), add that batch's results
            # to the overall result.
            for finished_future in as_completed(
                future_to_biosample_id_batch_map.keys()
            ):
                biosample_id_batch = future_to_biosample_id_batch_map[finished_future]
                logging.debug(
                    f"Finished processing batch of {len(biosample_id_batch)} `Biosample` IDs."
                )
                data_objects_by_biosample_id_in_batch = finished_future.result()
                data_objects_by_biosample_id.update(
                    data_objects_by_biosample_id_in_batch
                )

    # Convert the `data_objects_by_biosample_id` dictionary into a list of dicts;
    # i.e., into the format returned by the initial version of this API endpoint,
    # which did not use the `gather_linked_instances` function under the hood.
    num_data_objects = sum(len(dobs) for dobs in data_objects_by_biosample_id.values())
    logging.info(f"Found a total of {num_data_objects} DataObjects (not deduplicated).")
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
            examples=[r"nmdc:wfmag-11-00jn7876.1"],
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
        "**Note:** The data returned by this API endpoint can be up to 60 minutes out of date "
        "with respect to the NMDC database. That's because the cache that underlies this API "
        "endpoint gets refreshed to match the NMDC database once every 60 minutes."
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
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """This API endpoint retrieves resources related to the specified WorkflowExecution,
    including DataObjects that are inputs to — or outputs from — it, other WorkflowExecution
    instances that are part of the same pipeline, and related Biosamples and Studies.

    :param workflow_execution_id: id of workflow_execution_set instance for which related objects are to be retrieved
    :param mdb: A PyMongo `Database` instance that can be used to access the MongoDB database
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
