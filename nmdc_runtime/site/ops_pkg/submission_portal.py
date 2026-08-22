"""
Dagster ops related to the NMDC Submission Portal.

Note: These were extracted from a 1900-line file at `nmdc_runtime/site/ops.py` during a refactor.
"""

from enum import StrEnum
from pprint import pformat

from nmdc_schema.nmdc import SubmissionStatusEnum
from typing import Tuple
from dagster import (
    Any,
    Failure,
    MetadataValue,
    OpExecutionContext,
    Optional,
    Out,
    Output,
    op,
)
from nmdc_schema import nmdc

from nmdc_runtime.api.models.run import (
    RunEventType,
    RunSummary,
)
from nmdc_runtime.site.resources import (
    NmdcPortalApiClient,
    RuntimeApiSiteClient,
)
from nmdc_runtime.site.translation.submission_portal_translator import (
    SubmissionPortalTranslator,
)
from nmdc_runtime.site.util import get_instruments_by_id


class FinalizeSubmissionResult(StrEnum):
    FINALIZED = "finalized"
    ALREADY_LINKED = "already_linked"


@op(required_resource_keys={"mongo"})
def get_all_instruments(context: OpExecutionContext) -> dict[str, dict]:
    mdb = context.resources.mongo.db
    return get_instruments_by_id(mdb)


@op(required_resource_keys={"mongo"})
def get_instrument_ids_by_model(context: OpExecutionContext) -> dict[str, str]:
    mdb = context.resources.mongo.db
    instruments_by_id = get_instruments_by_id(mdb)
    instruments_by_model: dict[str, str] = {}
    for inst_id, instrument in instruments_by_id.items():
        model = instrument.get("model")
        if model is None:
            context.log.warning(f"Instrument {inst_id} has no model.")
            continue
        if model in instruments_by_model:
            context.log.warning(f"Instrument model {model} is not unique.")
        instruments_by_model[model] = inst_id
    context.log.info("Instrument models: %s", pformat(instruments_by_model))
    return instruments_by_model


@op(
    out={
        "submission_id": Out(),
        "sample_set_id": Out(),
        "nucleotide_sequencing_mapping_file_url": Out(Optional[str]),
        "data_object_mapping_file_url": Out(Optional[str]),
        "biosample_extras_file_url": Out(Optional[str]),
        "biosample_extras_slot_mapping_file_url": Out(Optional[str]),
        "study_id": Out(Optional[str]),
    },
)
def get_submission_portal_pipeline_inputs(
    context: OpExecutionContext,
    submission_id: str,
    sample_set_id: str,
    nucleotide_sequencing_mapping_file_url: str | None,
    data_object_mapping_file_url: str | None,
    biosample_extras_file_url: str | None,
    biosample_extras_slot_mapping_file_url: str | None,
    study_id: str | None,
) -> Tuple[str, str, str | None, str | None, str | None, str | None, str | None]:
    """Collect inputs required for translating a submission portal submission.

    This op defines required and optional inputs for translating a submission portal submission into
    NMDC schema records. The values are returned as a tuple. This is defined as an op so that
    multiple Dagster graphs can use the same input definition.
    """
    return (
        submission_id,
        sample_set_id,
        nucleotide_sequencing_mapping_file_url,
        data_object_mapping_file_url,
        biosample_extras_file_url,
        biosample_extras_slot_mapping_file_url,
        study_id,
    )


@op(
    required_resource_keys={"nmdc_portal_api_client"},
)
def fetch_nmdc_portal_submission_by_id(
    context: OpExecutionContext, submission_id: str
) -> dict[str, Any]:
    client: NmdcPortalApiClient = context.resources.nmdc_portal_api_client
    return client.fetch_metadata_submission(submission_id)


@op
def validate_submission_sample_set_id(
    metadata_submission: dict[str, Any], sample_set_id: str
) -> str:
    """Validate that the sample_set_id is associated with the metadata_submission.

    Some Dagster graphs accept both a submission ID and a sample set ID as inputs. It is expected
    that the sample set ID is associated with the submission. To prevent accidental mismatches (for
    example, a Dagster user re-uses a previous run configuration and forgets to update one of the
    IDs), this op checks that the sample set ID is included in the submission's sample sets. If it
    is not, a Failure is raised. If it is, the sample set ID is returned.
    """
    submission_id = metadata_submission.get("id")
    sample_sets = metadata_submission.get("sample_sets")

    if not isinstance(sample_sets, list):
        raise Failure(
            description=(
                f"Submission '{submission_id}' response does not include a sample_sets list."
            ),
            metadata={
                "submission_id": MetadataValue.text(str(submission_id)),
                "sample_set_id": MetadataValue.text(sample_set_id),
            },
        )

    submission_sample_set_ids = [
        sample_set["id"]
        for sample_set in sample_sets
        if isinstance(sample_set, dict) and sample_set.get("id") is not None
    ]

    if sample_set_id not in submission_sample_set_ids:
        raise Failure(
            description=(
                f"Sample set '{sample_set_id}' is not associated with submission '{submission_id}'."
            ),
            metadata={
                "submission_id": MetadataValue.text(str(submission_id)),
                "sample_set_id": MetadataValue.text(sample_set_id),
                "submission_sample_set_ids": MetadataValue.json(
                    submission_sample_set_ids
                ),
            },
        )

    return sample_set_id


@op(required_resource_keys={"nmdc_portal_api_client"})
def fetch_nmdc_portal_submission_sample_set_by_id(
    context: OpExecutionContext, sample_set_id: str
) -> dict[str, Any]:
    client: NmdcPortalApiClient = context.resources.nmdc_portal_api_client
    return client.fetch_sample_set(sample_set_id)


@op(required_resource_keys={"mongo", "runtime_api_site_client"})
def translate_portal_submission_to_nmdc_schema_database(
    context: OpExecutionContext,
    metadata_submission: dict[str, Any],
    sample_set: dict[str, Any],
    nucleotide_sequencing_mapping: list,
    data_object_mapping: list,
    instrument_mapping: dict[str, str],
    study_category: str | None,
    study_pi_image_url: str | None,
    biosample_extras: list[dict] | None,
    biosample_extras_slot_mapping: list[dict] | None,
    study_id: str | None,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    mdb = context.resources.mongo.db

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    existing_study: nmdc.Study | None = None
    # Attempt to find an existing study in Mongo to reuse instead of creating a new one.
    if study_id:
        # If a study_id is provided, check that it exists in Mongo. If it exists, use that one. If
        # it doesn't exist, raise an error.
        study_doc = mdb.study_set.find_one({"id": study_id}, {"_id": 0})
        if study_doc is None:
            raise Exception(f"Study with ID '{study_id}' does not exist in Mongo.")
        existing_study = nmdc.Study(**study_doc)
    elif metadata_submission.get("nmdc_study_id"):
        # If no study_id is provided but the submission has a nmdc_study_id value, check if a study
        # with that ID exists in Mongo. If it exists, use that one. If it doesn't exist, raise an
        # error.
        study_doc = mdb.study_set.find_one(
            {"id": metadata_submission["nmdc_study_id"]}, {"_id": 0}
        )
        if study_doc is None:
            raise Exception(
                f"Submission has nmdc_study_id '{metadata_submission['nmdc_study_id']}' but no Study with that ID exists in Mongo."
            )
        existing_study = nmdc.Study(**study_doc)

    translator = SubmissionPortalTranslator(
        metadata_submission,
        sample_set,
        existing_study=existing_study,
        nucleotide_sequencing_mapping=nucleotide_sequencing_mapping,
        data_object_mapping=data_object_mapping,
        id_minter=id_minter,
        study_category=study_category,
        study_pi_image_url=study_pi_image_url,
        biosample_extras=biosample_extras,
        biosample_extras_slot_mapping=biosample_extras_slot_mapping,
        illumina_instrument_mapping=instrument_mapping,
    )
    database = translator.get_database()
    return database


@op(
    required_resource_keys={"nmdc_portal_api_client"},
    out={
        "finalized_study_database": Out(is_required=False),
        "finalize_submission_result": Out(FinalizeSubmissionResult),
    },
)
def finalize_submission(
    context: OpExecutionContext,
    run_summary: RunSummary,
    database: nmdc.Database,
    metadata_submission: dict[str, Any],
):
    """Finalize a submission by calling the /api/metadata_submission/{submission_id}/finalize
    endpoint and using the response to add public image URLs to the translated nmdc:Study. When
    image URLs are added, emit a minimal nmdc:Database containing only the updated nmdc:Study so
    it can be persisted through /metadata/json:submit. Emit a finalize result only when the
    submission is known to be finalized or was already linked to an nmdc:Study. Emitting a
    finalize result allows downstream steps to finalize the sample set. The finalize result is
    not emitted when finalizing the sample set should be skipped.

    Skip this step if:
      - the preceding insert into MongoDB failed (as indicated by the run_summary.status not being
        RunEventType.COMPLETE)
      - the submission is already associated with an existing nmdc:Study (as indicated by having its
        nmdc_study_id field populated)
      - the provided nmdc:Database does not contain any nmdc:Study instances.
    """
    client: NmdcPortalApiClient = context.resources.nmdc_portal_api_client
    submission_id = metadata_submission["id"]

    if run_summary.status != RunEventType.COMPLETE:
        context.log.info(
            f"MongoDB insert for submission '{submission_id}' did not complete successfully; skipping submission finalization step."
        )
        return

    if metadata_submission.get("nmdc_study_id") is not None:
        nmdc_study_id = metadata_submission["nmdc_study_id"]
        context.log.info(
            f"Submission '{submission_id}' is already associated with nmdc:Study '{nmdc_study_id}'; skipping submission finalization step."
        )
        yield Output(
            FinalizeSubmissionResult.ALREADY_LINKED,
            output_name="finalize_submission_result",
        )
        return

    if database.study_set is None or len(database.study_set) == 0:
        context.log.info(
            "No studies in nmdc:Database; skipping submission finalization step."
        )
        return

    if len(database.study_set) > 1:
        context.log.warning(
            "Multiple studies in nmdc:Database; only finalizing the first study."
        )

    # Call the submission portal API to finalize the submission and yield a result indicating that
    # the submission was finalized. The API call returns optional public image URLs (if any were
    # uploaded for the submission). These will handled after the finalize result is yielded.
    study_id = database.study_set[0].id
    public_images = client.finalize_submission(submission_id, study_id=study_id)
    yield Output(
        FinalizeSubmissionResult.FINALIZED, output_name="finalize_submission_result"
    )

    # Check if any public image URLs were returned by the API call
    if not any(
        public_images.get(image_field)
        for image_field in (
            "pi_image_url",
            "primary_study_image_url",
            "study_image_urls",
        )
    ):
        # This is an expected case where the user did not upload any images for this submission. In
        # this case there is no further work to do. Just log a message and return.
        context.log.info(
            f"Submission '{submission_id}' finalization did not return public image URLs;"
            f"no updates to nmdc:Study '{study_id}' to make."
        )
        return

    # If any public image URLs were returned by the API call, update the nmdc:Study and yield a
    # minimal "finalized" nmdc:Database containing only the updated nmdc:Study so the image URLs can
    # be persisted through /metadata/json:submit.
    SubmissionPortalTranslator.set_study_images(
        database.study_set[0],
        public_images.get("pi_image_url"),
        public_images.get("primary_study_image_url"),
        public_images.get("study_image_urls"),
    )
    yield Output(
        nmdc.Database(study_set=[database.study_set[0]]),
        output_name="finalized_study_database",
    )


@op(required_resource_keys={"nmdc_portal_api_client"})
def finalize_sample_set(
    context: OpExecutionContext,
    run_summary: RunSummary,
    sample_set: dict[str, Any],
    finalize_submission_result: FinalizeSubmissionResult,
) -> None:
    """Finalize a sample set by calling the /api/metadata_submission/sample_set/{sample_set_id}/status
    endpoint to change the sample set's status to 'Released'.

    Skip this step if the preceding insert into MongoDB failed (as indicated by the
    run_summary.status not being RunEventType.COMPLETE). This step also depends on a submission
    finalize result emitted only after the parent submission has been finalized or was already
    linked to an nmdc:Study.
    """
    client: NmdcPortalApiClient = context.resources.nmdc_portal_api_client
    context.log.info(
        f"Submission finalize result satisfied: {finalize_submission_result}"
    )
    sample_set_id = sample_set["id"]

    if run_summary.status != RunEventType.COMPLETE:
        context.log.info(
            f"MongoDB insert for sample set '{sample_set_id}' did not complete successfully; skipping sample set finalization step."
        )
        return

    client.set_sample_set_status(
        sample_set_id, status=SubmissionStatusEnum.Released.text
    )
