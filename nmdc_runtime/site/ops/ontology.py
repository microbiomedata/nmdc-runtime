"""
Dagster ops related to the ontology loader.

Note: These were extracted from a 1900-line file at `nmdc_runtime/site/ops.py` during a refactor.
"""

import logging
import os
import re

from dagster import (
    Array,
    DagsterRunStatus,
    Failure,
    Field,
    In,
    Noneable,
    Nothing,
    OpExecutionContext,
    op,
    RunsFilter,
)
from dagster._core.execution.context.invocation import DirectOpExecutionContext

from ontology_loader.ontology_load_controller import OntologyLoaderController

LOAD_ONTOLOGY_MODES = {"meticulous", "fast-initial"}

# Statuses that mean a Dagster run is active or about to be: mirrors should_execute_ensure_alldocs
# in repository.py. Deliberately excludes SUCCESS/FAILURE/CANCELED/MANAGED (finished, not a
# concurrency risk).
_ACTIVE_RUN_STATUSES = [
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]


# Maps a load_ontology source_ontology value to the id prefix its docs use in
# ontology_class_set/ontology_relation_set. Used only to cross-check delete_ontology_terms_by_prefix
# and load_ontology aren't configured to target two different ontologies (see
# _fail_if_id_prefix_mismatches_load_ontology); not a general ontology registry.
_ONTOLOGY_ID_PREFIXES = {
    "ncbitaxon": "NCBITaxon:",
    "envo": "ENVO:",
    "uberon": "UBERON:",
    "po": "PO:",
}


def _fail_if_other_active_run(context: OpExecutionContext, job_names: list[str]):
    """
    Raise Failure if any run of the given job names, OTHER than this op's own run, is active.

    Unlike should_execute_ensure_alldocs (a schedule-level check that runs before a run exists),
    this runs from inside an already-started run, so it must exclude context.run_id from the
    count or every run would see itself and refuse to proceed.
    """
    for job_name in job_names:
        active_runs = context.instance.get_runs(
            filters=RunsFilter(job_name=job_name, statuses=_ACTIVE_RUN_STATUSES)
        )
        other_run_ids = [r.run_id for r in active_runs if r.run_id != context.run_id]
        if other_run_ids:
            raise Failure(
                f"Refusing to proceed: job {job_name!r} already has an active run "
                f"{other_run_ids}. This op and {job_name!r} write to the same shared ontology "
                "collections and must not run concurrently."
            )


def _fail_if_id_prefix_mismatches_load_ontology(
    context: OpExecutionContext, id_prefix: str
):
    """
    Raise Failure if this run's load_ontology step targets a different ontology than id_prefix.

    id_prefix (this op's delete target) and load_ontology's source_ontology (what gets loaded
    back afterward) are separate, independently-editable config fields with nothing in Dagster
    tying them together. reload_ontology_by_prefix is a manual, high-blast-radius job whose
    default config can be overridden at launch time, so a hand-edited run config could otherwise
    delete one ontology's docs and load a different one in their place. Only checks when a
    load_ontology step with a source_ontology in _ONTOLOGY_ID_PREFIXES is present in this run;
    silently skips otherwise (e.g. an unmapped ontology, or a future graph reusing just the
    delete op without a paired load_ontology step) rather than blocking on what it can't verify.
    """
    if isinstance(context, DirectOpExecutionContext):
        # context.run_config raises DagsterInvalidPropertyError on a directly-invoked op (no real
        # job run to read it from): nothing to cross-check in that case, e.g. this repo's own
        # single-op unit tests.
        return
    load_ontology_config = (
        context.run_config.get("ops", {}).get("load_ontology", {}).get("config", {})
    )
    source_ontology = load_ontology_config.get("source_ontology")
    if source_ontology is None:
        return
    expected_id_prefix = _ONTOLOGY_ID_PREFIXES.get(source_ontology)
    if expected_id_prefix is None:
        return
    if expected_id_prefix != id_prefix:
        raise Failure(
            f"Refusing to proceed: id_prefix {id_prefix!r} does not match the id prefix "
            f"{expected_id_prefix!r} expected for load_ontology's source_ontology "
            f"{source_ontology!r}. This run config would delete one ontology's docs and load a "
            "different ontology's docs in their place."
        )


@op(
    required_resource_keys={"mongo"},
    config_schema={
        # id_prefix: e.g. "NCBITaxon:". Classes are matched on `id` starting with this prefix;
        # relations are matched on `subject` starting with this prefix. Only correct for an
        # ontology whose classes and relations are cleanly separable by prefix with zero
        # cross-ontology entanglement -- verified for NCBITaxon (every class id and every
        # relation subject/object begins with "NCBITaxon:"; see nmdc-runtime issue 1565), not
        # assumed safe for any other ontology sharing these collections.
        "id_prefix": Field(str, is_required=True),
        "class_collection_name": Field(
            str, default_value="ontology_class_set", is_required=False
        ),
        "relation_collection_name": Field(
            str, default_value="ontology_relation_set", is_required=False
        ),
        # Names of Dagster jobs that must NOT have another active run while this op executes,
        # because they write to the same shared ontology collections (e.g. the regular scheduled
        # load of the same ontology, or another launch of this same reload job).
        "concurrent_job_names": Field(Array(str), default_value=[], is_required=False),
    },
)
def delete_ontology_terms_by_prefix(context: OpExecutionContext):
    """
    Delete an ontology's classes and relations from the shared ontology collections, by id prefix.

    The scoped drop-then-load recipe from nmdc-runtime issue 1565: since fast-initial mode has no
    upsert, refreshing an ontology loaded that way means deleting its existing docs first. This op
    only performs the delete; pair it with load_ontology (mode=fast-initial) in a graph to do the
    reload, e.g. via the `waits_for` Nothing-dependency load_ontology accepts.

    The prefix match is a case-sensitive, anchored ("^prefix") regex, which MongoDB can use as an
    index range scan rather than a full collection scan when a covering index exists. `id` is
    always indexed (nmdc_runtime.util.ensure_unique_id_indexes). `subject` is NOT indexed by
    anything in this repo, and the currently-pinned ontology-loader==0.2.3 does not create one
    either -- that lands in ontology-loader#60 (a unique (subject, predicate, object) index, whose
    subject-prefix MongoDB can use), unreleased as of this writing. Until a release containing that
    fix is pinned, the relation delete_many here is a full collection scan at whatever scale
    ontology_relation_set holds -- unacceptable at NCBITaxon's ~54.7M-relation scale. Confirm the
    index actually exists (or the pin has moved past 0.2.3) before running this against NCBITaxon.

    :return: {"class_collection_name": ..., "class_deleted_count": int,
        "relation_collection_name": ..., "relation_deleted_count": int}
    """
    cfg = context.op_config
    id_prefix = cfg["id_prefix"]
    if not id_prefix:
        # An empty prefix compiles to the regex "^", which matches every document. Refusing this
        # is the difference between a scoped delete and wiping every ontology in these shared
        # collections.
        raise Failure(
            "id_prefix must be a non-empty string; refusing to delete unscoped."
        )
    _fail_if_id_prefix_mismatches_load_ontology(context, id_prefix)
    class_collection_name = cfg.get("class_collection_name", "ontology_class_set")
    relation_collection_name = cfg.get(
        "relation_collection_name", "ontology_relation_set"
    )
    concurrent_job_names = cfg.get("concurrent_job_names", [])

    _fail_if_other_active_run(context, concurrent_job_names)

    db = context.resources.mongo.db
    prefix_pattern = re.compile(f"^{re.escape(id_prefix)}")

    class_result = db[class_collection_name].delete_many({"id": prefix_pattern})
    context.log.info(
        f"Deleted {class_result.deleted_count} classes from {class_collection_name!r} "
        f"matching id prefix {id_prefix!r}."
    )

    relation_result = db[relation_collection_name].delete_many(
        {"subject": prefix_pattern}
    )
    context.log.info(
        f"Deleted {relation_result.deleted_count} relations from {relation_collection_name!r} "
        f"matching subject prefix {id_prefix!r}."
    )

    return {
        "class_collection_name": class_collection_name,
        "class_deleted_count": class_result.deleted_count,
        "relation_collection_name": relation_collection_name,
        "relation_deleted_count": relation_result.deleted_count,
    }


@op(
    required_resource_keys={"mongo"},
    ins={"waits_for": In(dagster_type=Nothing)},
    config_schema={
        "source_ontology": str,
        # mode: "meticulous" = linkml-store per-item upsert, for incremental weekly
        #   re-loads; "fast-initial" = raw pymongo insert_many, for the one-time bulk
        #   install of a large ontology (e.g. NCBITaxon, ~2.7M classes). Required (not
        #   defaulted) so every schedule/job states its mode explicitly rather than
        #   silently falling back to the per-item-upsert path.
        "mode": Field(str, is_required=True),
        # closure: which ancestry closures to emit ("combined" = rdfs:subClassOf + BFO:0000050).
        "closure": Field(str, default_value="combined", is_required=False),
        # report_directory: only used when mode="meticulous" (TSV reports). When None it
        #   defaults to <cwd>/ontology_reports for meticulous; fast-initial writes no reports.
        "report_directory": Field(Noneable(str), default_value=None, is_required=False),
        # Names of Dagster jobs that must NOT have another active run while this op executes.
        # The reciprocal of delete_ontology_terms_by_prefix's own guard: that op already refuses
        # to start while a job named here is active, but without this, nothing stopped one of
        # those jobs from starting immediately afterward and inserting into a collection a
        # concurrent scoped reload is mid-delete on. Defaults to empty (no-op) so envo/uberon/po
        # schedules, which have no reload job to race against, are unaffected.
        "concurrent_job_names": Field(Array(str), default_value=[], is_required=False),
    },
)
def load_ontology(context: OpExecutionContext):
    cfg = context.op_config
    source_ontology = cfg["source_ontology"]
    mode = cfg["mode"]
    if mode not in LOAD_ONTOLOGY_MODES:
        raise ValueError(
            f"Invalid mode {mode!r} for load_ontology (source_ontology={source_ontology!r}): "
            f"must be one of {sorted(LOAD_ONTOLOGY_MODES)}."
        )
    _fail_if_other_active_run(context, cfg.get("concurrent_job_names", []))
    closure = cfg.get("closure", "combined")
    report_directory = cfg.get("report_directory")
    # Preserve the pre-0.2.3 report location for meticulous runs (unchanged behavior
    # for envo/uberon/po). fast-initial writes no reports, so leave it None there.
    if report_directory is None and mode == "meticulous":
        report_directory = os.path.join(os.getcwd(), "ontology_reports")

    # Redirect Python logging to Dagster context
    handler = logging.Handler()
    handler.emit = lambda record: context.log.info(record.getMessage())

    # Get logger from ontology-loader package
    controller_logger = logging.getLogger("ontology_loader.ontology_load_controller")
    controller_logger.setLevel(logging.INFO)
    controller_logger.addHandler(handler)

    context.log.info(
        f"Running Ontology Loader for ontology: {source_ontology} "
        f"(mode={mode}, closure={closure})"
    )
    loader = OntologyLoaderController(
        source_ontology=source_ontology,
        mode=mode,
        closure=closure,
        report_directory=report_directory,
        mongo_client=context.resources.mongo.client,
        db_name=context.resources.mongo.db.name,
    )

    loader.run_ontology_loader()
    context.log.info(f"Ontology load for {source_ontology} completed successfully!")
