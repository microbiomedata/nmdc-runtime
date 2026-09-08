"""
Tests targeting the Dagster op named `synchronize_superseded_by_field_op`, which updates the
`superseded_by` fields of workflow executions and data objects to reflect the supersession chains
implied by the workflow execution IDs.
"""

from collections.abc import Iterator
from datetime import datetime
from uuid import uuid4

from pymongo.database import Database
import pytest
from dagster import build_op_context
from dagster._core.execution.context.invocation import DirectOpExecutionContext

from nmdc_runtime.site.ops.superseded_by import synchronize_superseded_by_field_op
from nmdc_runtime.site.resources import mongo_resource
from tests.lib.faker import Faker


@pytest.fixture
def op_exec_ctx_having_empty_db(client_config) -> Iterator[DirectOpExecutionContext]:
    """
    Pytest fixture that builds and yields a Dagster op execution context that provides access to an
    empty, temporary MongoDB database, which the fixture will subsequently delete (i.e. clean up).
    """
    now = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g. 20261031_123059
    unique_database_name = f"_test_{now}_{uuid4().hex}"
    config = {**client_config, "dbname": unique_database_name}
    with build_op_context(
        resources={"mongo": mongo_resource.configured(config)}
    ) as op_execution_context:
        mongo = op_execution_context.resources.mongo
        if unique_database_name in mongo.client.list_database_names():
            raise ValueError("Database name is already in use. Inconceivable!")
        try:
            yield op_execution_context
        finally:
            mongo.client.drop_database(mongo.db.name)
            mongo.client.close()


def test_synchronize_superseded_by_field_op(op_exec_ctx_having_empty_db):
    """
    Seeds the database with workflow executions and data objects, some of which have `superseded_by`
    fields that are not consistent with the WFE IDs, and then runs the Dagster op and confirms those
    fields have been made consistent with the WFE IDs.
    """

    db: Database = op_exec_ctx_having_empty_db.resources.mongo.db
    faker = Faker()

    # IDs for a sequence of WorkflowExecutions (non-gap, and gap, between run numbers).
    wfe_id_1_1 = "nmdc:wfmgan-00-000001.1"
    wfe_id_1_2 = "nmdc:wfmgan-00-000001.2"
    wfe_id_1_10 = "nmdc:wfmgan-00-000001.10"

    # IDs for a different sequence of WorkflowExecutions (gap between run numbers).
    wfe_id_2_1 = "nmdc:wfmgan-00-000002.1"
    wfe_id_2_3 = "nmdc:wfmgan-00-000002.3"

    # ID for a "lone" WorkflowExecution (no other runs have this base ID).
    wfe_id_3_7 = "nmdc:wfmgan-00-000003.7"

    # IDs for DataObjects (at least some of which will appear in the `has_output` fields of WFEs).
    dobj_id_1 = "nmdc:dobj-00-000001"
    dobj_id_2 = "nmdc:dobj-00-000002"
    dobj_id_3 = "nmdc:dobj-00-000003"
    dobj_id_4 = "nmdc:dobj-00-000004"
    dobj_id_5 = "nmdc:dobj-00-000005"
    dobj_id_6 = "nmdc:dobj-00-000006"
    dobj_id_7 = "nmdc:dobj-00-000007"
    dobj_id_8 = "nmdc:dobj-00-000008"

    # Generate WorkflowExecutions having specific `id`, `has_output`, and `superseded_by` values.
    workflow_execution_field_overrides = [
        # WFE 1.1 has_output DOBJ 1 and 2, and does not say superseded_by anything. ⚠️ Expecting: superseded_by WFE 1.2.
        {
            "id": wfe_id_1_1,
            "has_output": [dobj_id_1, dobj_id_2],
        },
        # WFE 1.2 has_output DOBJ 3, and says superseded_by WFE 2.1. ⚠️ Expecting: superseded_by WFE 1.10.
        {
            "id": wfe_id_1_2,
            "has_output": [dobj_id_3],
            "superseded_by": wfe_id_2_1,
        },
        # WFE 1.10 has_output DOBJ 4, and says superseded by WFE 1.1. ⚠️ Expecting: no superseded_by field.
        {
            "id": wfe_id_1_10,
            "has_output": [dobj_id_4],
            "superseded_by": wfe_id_1_1,
        },
        # WFE 2.1 has_output DOBJ 5, and says superseded_by WFE 2.3. ✅
        {
            "id": wfe_id_2_1,
            "has_output": [dobj_id_5],
            "superseded_by": wfe_id_2_3,
        },
        # WFE 2.3 has_output DOBJ 6, and does not say superseded_by anything. ✅
        {
            "id": wfe_id_2_3,
            "has_output": [dobj_id_6],
        },
        # WFE 3.7 has no output, and says superseded_by nothing. ⚠️ Expecting: no superseded_by field.
        {
            "id": wfe_id_3_7,
            "superseded_by": None,
        },
    ]
    initial_workflow_executions: list[dict] = []
    for field_overrides in workflow_execution_field_overrides:
        workflow_execution = faker.generate_metagenome_annotations(
            quantity=1,
            was_informed_by=["nmdc:dgns-00-000001"],
            has_input=["nmdc:bsm-00-000001"],
            **field_overrides,
        )[0]
        initial_workflow_executions.append(workflow_execution)

    # Generate DataObjects having specific `id` and `superseded_by` values.
    data_object_field_overrides = [
        # ⚠️ Expecting: superseded_by WFE 1.2.
        {
            "id": dobj_id_1,
        },
        # ⚠️ Expecting: superseded_by WFE 1.2.
        {
            "id": dobj_id_2,
            "superseded_by": wfe_id_2_1,
        },
        # ⚠️ Expecting: superseded_by WFE 1.10.
        {
            "id": dobj_id_3,
            "superseded_by": None,
        },
        # ⚠️ Expecting: no superseded_by field.
        {
            "id": dobj_id_4,
            "superseded_by": wfe_id_1_1,
        },
        # ✅ Already says superseded_by WFE 2.3.
        {
            "id": dobj_id_5,
            "superseded_by": wfe_id_2_3,
        },
        # ⚠️ Expecting: no superseded_by field.
        {
            "id": dobj_id_6,
            "superseded_by": None,
        },
        # ⚠️ Expecting: no superseded_by field.
        {
            "id": dobj_id_7,
            "superseded_by": wfe_id_1_10,
        },
        # ⚠️ Expecting: no superseded_by field.
        {
            "id": dobj_id_8,
            "superseded_by": None,
        },
    ]
    initial_data_objects: list[dict] = []
    for field_overrides in data_object_field_overrides:
        data_object = faker.generate_data_objects(
            quantity=1,
            **field_overrides,
        )[0]
        initial_data_objects.append(data_object)

    # Insert the fake data into the database.
    db.workflow_execution_set.insert_many(initial_workflow_executions)
    db.data_object_set.insert_many(initial_data_objects)

    # Note: We use `False` here as a sentinel value to indicate that the field is missing.
    expected_wfe_superseded_by_values = {
        wfe_id_1_1: wfe_id_1_2,
        wfe_id_1_2: wfe_id_1_10,
        wfe_id_1_10: False,
        wfe_id_2_1: wfe_id_2_3,
        wfe_id_2_3: False,
        wfe_id_3_7: False,
    }
    expected_dobj_superseded_by_values = {
        dobj_id_1: wfe_id_1_2,
        dobj_id_2: wfe_id_1_2,
        dobj_id_3: wfe_id_1_10,
        dobj_id_4: False,
        dobj_id_5: wfe_id_2_3,
        dobj_id_6: False,
        dobj_id_7: False,
        dobj_id_8: False,
    }

    # Run the Dagster op in "dry run" mode and confirm the Mongo documents are still in their initial state.
    with build_op_context(
        resources={"mongo": op_exec_ctx_having_empty_db.resources.mongo},
        op_config={"dry_run": True},
    ) as dry_run_context:
        synchronize_superseded_by_field_op(dry_run_context)
    for initial_wfe in initial_workflow_executions:
        wfe_id = initial_wfe["id"]
        assert initial_wfe == db.workflow_execution_set.find_one({"id": wfe_id})
    for initial_dobj in initial_data_objects:
        dobj_id = initial_dobj["id"]
        assert initial_dobj == db.data_object_set.find_one({"id": dobj_id})

    # Run the Dagster op with its default config (i.e. not in "dry run" mode) to apply the changes.
    synchronize_superseded_by_field_op(op_exec_ctx_having_empty_db)

    # Compare the workflow executions with our expectations.
    for initial_wfe in initial_workflow_executions:
        wfe_id = initial_wfe["id"]
        expected_wfe = dict(initial_wfe)  # creates a shallow copy

        # Either populate or remove the `superseded_by` field, in our expectation dictionary.
        expected_superseded_by_value = expected_wfe_superseded_by_values[wfe_id]
        if isinstance(expected_superseded_by_value, str):
            expected_wfe["superseded_by"] = expected_superseded_by_value
        else:
            expected_wfe.pop("superseded_by", None)

        assert expected_wfe == db.workflow_execution_set.find_one({"id": wfe_id})

    # Compare the data objects with our expectations.
    for initial_dobj in initial_data_objects:
        dobj_id = initial_dobj["id"]
        expected_dobj = dict(initial_dobj)  # creates a shallow copy

        # Either populate or remove the `superseded_by` field, in our expectation dictionary.
        expected_superseded_by_value = expected_dobj_superseded_by_values[dobj_id]
        if isinstance(expected_superseded_by_value, str):
            expected_dobj["superseded_by"] = expected_superseded_by_value
        else:
            expected_dobj.pop("superseded_by", None)

        assert expected_dobj == db.data_object_set.find_one({"id": dobj_id})
