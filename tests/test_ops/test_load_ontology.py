import os
import pytest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
from dagster import (
    build_op_context,
    DagsterInvalidConfigError,
    DagsterRun,
    DagsterRunStatus,
    Failure,
)
from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.site.ops.ontology import (
    load_ontology,
    delete_ontology_terms_by_prefix,
    _fail_if_id_prefix_mismatches_load_ontology,
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


@pytest.fixture
def client_config():
    # Print details about the MongoDB configuration for debugging
    mongo_host = os.getenv("MONGO_HOST")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    mongo_username = os.getenv("MONGO_USERNAME")

    logging.info("Test MongoDB connection details:")
    logging.info(f"- MONGO_HOST: {mongo_host}")
    logging.info(f"- MONGO_DBNAME: {mongo_dbname}")
    logging.info(f"- MONGO_USERNAME: {mongo_username}")

    # For local development outside Docker, try connecting to the Docker-exposed port
    if mongo_host == "mongodb://mongo:27017":
        alternative_host = "mongodb://localhost:27018"
        logging.info(
            f"- Inside test: MongoDB host is set to container name. "
            f"If running test locally (not in Docker), try: {alternative_host}"
        )

    return {
        "dbname": mongo_dbname,
        "host": mongo_host,
        "password": os.getenv("MONGO_PASSWORD"),
        "username": mongo_username,
    }


@pytest.fixture
def op_context(client_config, tmp_path):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "envo",
            "mode": "meticulous",
            "closure": "combined",
            "report_directory": str(tmp_path),
        },
    )


# This test will always run - it doesn't require MongoDB connection
@patch("nmdc_runtime.site.ops.ontology.OntologyLoaderController")
def test_load_ontology(mock_ontology_loader, op_context):
    """Tests the load_ontology op using mocks to verify parameter passing and method calling"""
    # Setup the mock
    mock_instance = MagicMock()
    mock_ontology_loader.return_value = mock_instance

    # Call the function
    result = load_ontology(op_context)

    # Verify the correct parameters were used to initialize OntologyLoaderController
    mock_ontology_loader.assert_called_once_with(
        source_ontology="envo",
        mode="meticulous",
        closure="combined",
        report_directory=op_context.op_config["report_directory"],
        mongo_client=op_context.resources.mongo.client,
        db_name=op_context.resources.mongo.db.name,
    )

    # Verify that run_ontology_loader was called
    mock_instance.run_ontology_loader.assert_called_once()

    # The function doesn't have a return value
    assert result is None


@pytest.fixture
def op_context_invalid_mode(client_config):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "envo",
            "mode": "meticulously",  # typo: not a valid mode
            "closure": "combined",
        },
    )


@patch("nmdc_runtime.site.ops.ontology.OntologyLoaderController")
def test_load_ontology_invalid_mode_raises(
    mock_ontology_loader, op_context_invalid_mode
):
    """An unrecognized mode value must raise, not silently fall through."""
    with pytest.raises(ValueError, match="Invalid mode"):
        load_ontology(op_context_invalid_mode)
    mock_ontology_loader.assert_not_called()


@pytest.fixture
def op_context_invalid_closure(client_config):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "envo",
            "mode": "meticulous",
            "closure": "issa",  # typo: not a valid closure
        },
    )


@patch("nmdc_runtime.site.ops.ontology.OntologyLoaderController")
def test_load_ontology_invalid_closure_raises(
    mock_ontology_loader, op_context_invalid_closure
):
    """
    An unrecognized closure value must raise before OntologyLoaderController is even
    constructed. Copilot review 5045824839: ontology-loader 0.2.3 doesn't reject an invalid
    closure until after extracting every class, which for NCBITaxon (~2.7M classes) means a
    launch typo burns substantial time and memory before failing.
    """
    with pytest.raises(ValueError, match="Invalid closure"):
        load_ontology(op_context_invalid_closure)
    mock_ontology_loader.assert_not_called()


@pytest.fixture
def op_context_fast_initial(client_config):
    # No report_directory: fast-initial writes no reports, so the op should leave it None.
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "ncbitaxon",
            "mode": "fast-initial",
            "closure": "isa",
        },
    )


# Always runs - no MongoDB connection needed.
@patch("nmdc_runtime.site.ops.ontology.OntologyLoaderController")
def test_load_ontology_fast_initial(mock_ontology_loader, op_context_fast_initial):
    """fast-initial: op passes mode/closure through and leaves report_directory=None."""
    mock_instance = MagicMock()
    mock_ontology_loader.return_value = mock_instance

    result = load_ontology(op_context_fast_initial)

    mock_ontology_loader.assert_called_once_with(
        source_ontology="ncbitaxon",
        mode="fast-initial",
        closure="isa",
        report_directory=None,
        mongo_client=op_context_fast_initial.resources.mongo.client,
        db_name=op_context_fast_initial.resources.mongo.db.name,
    )
    mock_instance.run_ontology_loader.assert_called_once()
    assert result is None


@patch("nmdc_runtime.site.ops.ontology.OntologyLoaderController")
def test_load_ontology_raises_on_other_active_run(mock_ontology_loader, client_config):
    """
    load_ontology must refuse to start while a job named in concurrent_job_names is active.

    The reciprocal of delete_ontology_terms_by_prefix's own guard: without this, the scheduled
    weekly load could start immediately after a scoped reload's delete-step check passed, and
    insert into a collection the reload is concurrently deleting from.
    """
    context = build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)},
        op_config={
            "source_ontology": "ncbitaxon",
            "mode": "fast-initial",
            "closure": "isa",
            "concurrent_job_names": ["reload_ncbitaxon_ontology"],
        },
    )
    other_run = DagsterRun(
        job_name="reload_ncbitaxon_ontology",
        run_id="other-run-id",
        status=DagsterRunStatus.STARTED,
    )
    context.instance.get_runs = MagicMock(return_value=[other_run])

    with pytest.raises(Failure, match="reload_ncbitaxon_ontology"):
        load_ontology(context)

    mock_ontology_loader.assert_not_called()


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_load_ontology_integration(op_context):
    """Tests the load_ontology op with actual MongoDB connection and verifies results"""

    # Get MongoDB client
    mdb = op_context.resources.mongo.db

    # Print detail about the MongoDB connection
    logging.info(
        f"Connected to MongoDB: {op_context.resources.mongo.db.client.address}"
    )

    # Check if ontology collections exist before running
    ontology_class_set_before = mdb.get_collection(
        "ontology_class_set"
    ).count_documents({})
    ontology_relation_set_before = mdb.get_collection(
        "ontology_relation_set"
    ).count_documents({})

    logging.info(
        f"Before running: {ontology_class_set_before} classes, {ontology_relation_set_before} relations"
    )

    # Execute the op
    result = load_ontology(op_context)

    # Verify the op behavior:
    # 1. Check that ontology_class_set has entries
    ontology_class_count = mdb.get_collection("ontology_class_set").count_documents({})
    logging.info(f"After running: {ontology_class_count} classes")

    # 2. Check that ontology_relation_set has entries
    ontology_relation_count = mdb.get_collection(
        "ontology_relation_set"
    ).count_documents({})
    logging.info(f"After running: {ontology_relation_count} relations")

    # 3. Check for some known ENVO terms if we have ontology data
    assert ontology_class_count > 0
    assert ontology_relation_count > 0

    sample_envo_id = "ENVO:00000001"  # Example ENVO ID
    envo_term = mdb.get_collection("ontology_class_set").find_one(
        {"id": sample_envo_id}
    )
    assert envo_term is not None

    # 4. Check report files (only the "meticulous" mode writes TSV reports)
    if op_context.op_config["mode"] == "meticulous":
        output_dir = op_context.op_config["report_directory"]
        assert os.path.exists(os.path.join(output_dir, "ontology_inserts.tsv"))
        assert os.path.exists(os.path.join(output_dir, "ontology_updates.tsv"))

    # 5. Verify the function has no return value (was incorrectly expected to be 0)
    assert result is None


# --- delete_ontology_terms_by_prefix ------------------------------------------------------------


def _mock_mongo_context(op_config, instance_get_runs_return=None):
    """
    Build an op context with a mocked mongo resource's raw `db[...]` bracket access, and a mocked
    `context.instance.get_runs` (the concurrency guard's own dependency, not something this op
    owns, so mocked rather than exercised against a real DagsterInstance -- matches this test
    file's existing style of mocking OntologyLoaderController rather than a real Mongo/ontology).

    `build_op_context()` doesn't accept a run_id override; `context.run_id` is always the literal
    string "EPHEMERAL" for a direct-invocation test context. Tests exercising the concurrency
    guard use that exact value for "this op's own run" and a different id for "another run".
    """
    mock_db = MagicMock()
    mock_client_config = {
        "dbname": "test_db",
        "host": "mongodb://localhost:27017",
        "password": "x",
        "username": "x",
    }
    context = build_op_context(
        resources={"mongo": mongo_resource.configured(mock_client_config)},
        op_config=op_config,
    )
    context.resources.mongo.db = mock_db
    context.instance.get_runs = MagicMock(return_value=instance_get_runs_return or [])
    return context, mock_db


def test_delete_ontology_terms_by_prefix_happy_path():
    """Deletes classes by id prefix and relations by subject prefix; returns both counts."""
    context, mock_db = _mock_mongo_context(
        op_config={"id_prefix": "NCBITaxon:"},
    )
    mock_db.__getitem__.return_value.delete_many.side_effect = [
        MagicMock(deleted_count=2708804),  # ontology_class_set
        MagicMock(deleted_count=54700052),  # ontology_relation_set
    ]

    result = delete_ontology_terms_by_prefix(context)

    assert result == {
        "class_collection_name": "ontology_class_set",
        "class_deleted_count": 2708804,
        "relation_collection_name": "ontology_relation_set",
        "relation_deleted_count": 54700052,
    }
    calls = mock_db.__getitem__.return_value.delete_many.call_args_list
    assert "id" in calls[0].args[0]
    assert "subject" in calls[1].args[0]


def test_delete_ontology_terms_by_prefix_rejects_empty_id_prefix():
    """
    An empty id_prefix compiles to the regex "^", matching every document.

    Without this guard, a config typo (empty string) against this destructive op would delete
    every ontology in these shared collections, not just the one intended.
    """
    context, mock_db = _mock_mongo_context(op_config={"id_prefix": ""})

    with pytest.raises(Failure, match="non-empty"):
        delete_ontology_terms_by_prefix(context)

    mock_db.__getitem__.return_value.delete_many.assert_not_called()


def test_delete_ontology_terms_by_prefix_collection_names_are_fixed():
    """
    The op always targets ontology_class_set/ontology_relation_set, hardcoded, not read from cfg.

    Copilot review 5045965421 (PR 1562): load_ontology/ontology-loader has no matching override,
    always writing back to ontology_class_set/ontology_relation_set. An independent override on
    this op alone would let a valid-looking config delete from one collection pair while
    load_ontology reloads into a different one, so the override capability was removed entirely
    rather than validated against a sibling op's config.
    """
    context, mock_db = _mock_mongo_context(op_config={"id_prefix": "TEST:"})
    mock_db.__getitem__.return_value.delete_many.return_value = MagicMock(
        deleted_count=0
    )

    result = delete_ontology_terms_by_prefix(context)

    assert result["class_collection_name"] == "ontology_class_set"
    assert result["relation_collection_name"] == "ontology_relation_set"
    accessed_names = [c.args[0] for c in mock_db.__getitem__.call_args_list]
    assert accessed_names == ["ontology_class_set", "ontology_relation_set"]


def test_delete_ontology_terms_by_prefix_rejects_collection_name_config():
    """
    class_collection_name/relation_collection_name are no longer valid config keys at all.

    Dagster's own config-schema validation now rejects an attempt to set them, before the op
    body even runs -- a stronger guarantee than the op merely ignoring them.
    """
    context, _ = _mock_mongo_context(
        op_config={
            "id_prefix": "TEST:",
            "class_collection_name": "custom_class_set",
            "relation_collection_name": "custom_relation_set",
        },
    )

    # Config validation against the op's schema is deferred until the op is actually invoked
    # with this context, not at build_op_context() construction time.
    with pytest.raises(DagsterInvalidConfigError, match="class_collection_name"):
        delete_ontology_terms_by_prefix(context)


def test_delete_ontology_terms_by_prefix_raises_on_other_active_run():
    """
    The concurrency guard must raise Failure when another run of a named job is active.

    This is the scenario the guard exists for: an operator double-launches the reload job, or
    launches it while the regular weekly NCBITaxon load happens to be running.
    """
    other_run = DagsterRun(
        job_name="scheduled_ncbitaxon_ontology_load",
        run_id="other-run-id",
        status=DagsterRunStatus.STARTED,
    )
    context, mock_db = _mock_mongo_context(
        op_config={
            "id_prefix": "NCBITaxon:",
            "concurrent_job_names": ["scheduled_ncbitaxon_ontology_load"],
        },
        instance_get_runs_return=[other_run],
    )

    with pytest.raises(Failure, match="scheduled_ncbitaxon_ontology_load"):
        delete_ontology_terms_by_prefix(context)

    mock_db.__getitem__.return_value.delete_many.assert_not_called()


def test_delete_ontology_terms_by_prefix_does_not_raise_on_only_own_run():
    """
    The guard must exclude the op's own in-progress run from the "other active run" count.

    Without excluding context.run_id, every run of a guarded job would see itself in the active
    list and refuse to ever proceed.
    """
    own_run = DagsterRun(
        job_name="reload_ncbitaxon_ontology",
        run_id="EPHEMERAL",  # matches context.run_id for a build_op_context() test context
        status=DagsterRunStatus.STARTED,
    )
    context, mock_db = _mock_mongo_context(
        op_config={
            "id_prefix": "NCBITaxon:",
            "concurrent_job_names": ["reload_ncbitaxon_ontology"],
        },
        instance_get_runs_return=[own_run],
    )
    mock_db.__getitem__.return_value.delete_many.return_value = MagicMock(
        deleted_count=0
    )

    result = delete_ontology_terms_by_prefix(context)  # must not raise

    assert result["class_deleted_count"] == 0


def test_delete_ontology_terms_by_prefix_guard_is_noop_when_no_job_names_configured():
    """concurrent_job_names defaults to empty: no guard check, no instance.get_runs call at all."""
    context, mock_db = _mock_mongo_context(op_config={"id_prefix": "NCBITaxon:"})
    mock_db.__getitem__.return_value.delete_many.return_value = MagicMock(
        deleted_count=0
    )

    delete_ontology_terms_by_prefix(context)  # must not raise

    context.instance.get_runs.assert_not_called()


def test_fail_if_id_prefix_mismatches_load_ontology_raises_on_mismatch():
    """id_prefix must match the id prefix load_ontology's source_ontology will load back."""
    context = SimpleNamespace(
        run_config={"ops": {"load_ontology": {"config": {"source_ontology": "envo"}}}}
    )
    with pytest.raises(Failure, match="does not match"):
        _fail_if_id_prefix_mismatches_load_ontology(context, "NCBITaxon:")


def test_fail_if_id_prefix_mismatches_load_ontology_passes_on_match():
    context = SimpleNamespace(
        run_config={
            "ops": {"load_ontology": {"config": {"source_ontology": "ncbitaxon"}}}
        }
    )
    _fail_if_id_prefix_mismatches_load_ontology(context, "NCBITaxon:")  # must not raise


def test_fail_if_id_prefix_mismatches_load_ontology_raises_on_unmapped_ontology():
    """
    An unrecognized source_ontology must fail closed, not skip. Copilot review 5045225079 caught
    this: silently skipping meant a typo (or any value outside _ONTOLOGY_ID_PREFIXES) sailed
    through the check this function exists to enforce, on a destructive, high-blast-radius job.
    """
    context = SimpleNamespace(
        run_config={
            "ops": {
                "load_ontology": {"config": {"source_ontology": "some_future_ontology"}}
            }
        }
    )
    with pytest.raises(Failure, match="not a recognized ontology"):
        _fail_if_id_prefix_mismatches_load_ontology(context, "ANYTHING:")


def test_fail_if_id_prefix_mismatches_load_ontology_skips_when_load_ontology_absent():
    """No load_ontology step in this run's config (e.g. a future graph reusing only the delete
    op): nothing to cross-check against, so this must not raise."""
    context = SimpleNamespace(run_config={"ops": {}})
    _fail_if_id_prefix_mismatches_load_ontology(context, "NCBITaxon:")  # must not raise


def test_delete_ontology_terms_by_prefix_direct_invocation_skips_mismatch_check():
    """
    A directly-invoked op context (this file's own unit-test style) has no real job run to read
    run_config from, so the mismatch cross-check must skip rather than raise
    DagsterInvalidPropertyError. Configures id_prefix to an ontology-implying value with no
    load_ontology step anywhere in this test's context, matching how every other test in this
    file invokes delete_ontology_terms_by_prefix directly.
    """
    context, mock_db = _mock_mongo_context(op_config={"id_prefix": "NCBITaxon:"})
    mock_db.__getitem__.return_value.delete_many.return_value = MagicMock(
        deleted_count=0
    )

    delete_ontology_terms_by_prefix(context)  # must not raise


def test_scheduled_ncbitaxon_ontology_load_guards_against_its_own_concurrent_runs():
    """
    Copilot review 5045560752 caught it: weekly_load_ncbitaxon_ontology's concurrent_job_names
    only listed reload_ncbitaxon_ontology, not its own job name. _fail_if_other_active_run
    already excludes context.run_id (the current run), so including a job's own name only
    catches a genuinely separate concurrent launch -- without it, two manual triggers of this
    fast-initial (no upsert) job could overlap and each bulk-insert the same ~2.7M classes.
    """
    from nmdc_runtime.site.repository import load_ncbitaxon_ontology_weekly

    concurrent_job_names = load_ncbitaxon_ontology_weekly.job.run_config["ops"][
        "load_ontology"
    ]["config"]["concurrent_job_names"]
    assert "scheduled_ncbitaxon_ontology_load" in concurrent_job_names


@pytest.mark.parametrize(
    "ontology,id_prefix",
    [("envo", "ENVO:"), ("uberon", "UBERON:"), ("po", "PO:")],
)
def test_reload_ontology_jobs_target_the_right_ontology(ontology, id_prefix):
    """
    reload_ontology_by_prefix generalized to envo/uberon/po (previously NCBITaxon-only): each
    reload job's delete step and load step must target the same ontology, and each weekly
    schedule must guard against its own reload job the same way NCBITaxon's does.
    """
    from nmdc_runtime.site import repository

    reload_job = getattr(repository, f"reload_{ontology}_ontology_job")
    weekly_schedule = getattr(repository, f"load_{ontology}_ontology_weekly")

    reload_cfg = reload_job.run_config["ops"]
    assert (
        reload_cfg["delete_ontology_terms_by_prefix"]["config"]["id_prefix"]
        == id_prefix
    )
    assert reload_cfg["load_ontology"]["config"]["source_ontology"] == ontology
    assert reload_cfg["load_ontology"]["config"]["mode"] == "fast-initial"

    weekly_concurrent = weekly_schedule.job.run_config["ops"]["load_ontology"][
        "config"
    ]["concurrent_job_names"]
    assert f"reload_{ontology}_ontology" in weekly_concurrent
    assert f"scheduled_{ontology}_ontology_load" in weekly_concurrent
