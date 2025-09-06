import fastjsonschema
import pytest
from fastjsonschema import JsonSchemaValueException
from toolz import dissoc

from nmdc_runtime.api.db.mongo import get_nonempty_nmdc_schema_collection_names
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import get_nmdc_jsonschema_dict


@pytest.mark.skip(reason=r"""
Exception
----------------------------- Captured stdout call -----------------------------
testing schema conformance for biosample_set ...
testing schema conformance for data_generation_set ...
testing schema conformance for material_processing_set ...
testing schema conformance for study_set ...
testing schema conformance for workflow_execution_set ...
6 fails
failed: biosample_set doc with id nmdc:bsm-12-7mysck21 (data.biosample_set[0] must contain ['associated_studies'] properties)
failed: biosample_set doc with id nmdc:bsm-11-5nhz3402 (data.biosample_set[0] must contain ['associated_studies'] properties)
failed: data_generation_set doc with id nmdc:omprc-11-00383810 (data.data_generation_set[0] cannot be validated by any definition)
failed: study_set doc with id nmdc:sty-1-foobar (data.study_set[0] must contain ['study_category'] properties)
failed: workflow_execution_set doc with id nmdc:wfmag-11-00jn7876.2 (data.workflow_execution_set[0] cannot be validated by any definition)
failed: workflow_execution_set doc with id nmdc:wfmag-11-0133pz73.1 (data.workflow_execution_set[0] cannot be validated by any definition)
""".strip())
def test_schema_conformance():
    """
    TODO: Document this test. Was its author trying to check that all documents in
          a test database conform to the schema, except disregarding whether their
          IDs conform to ID patterns in the schema?
    """
    mdb = get_mongo(run_config_frozen__normal_env).db
    names = get_nonempty_nmdc_schema_collection_names(mdb)
    fails = []
    nmdc_jsonschema_validator = fastjsonschema.compile(
        get_nmdc_jsonschema_dict(enforce_id_patterns=False)
    )
    for name in sorted(names):
        print(f"testing schema conformance for {name} ...")
        for d in mdb[name].find(limit=10):
            try:
                nmdc_jsonschema_validator({name: [dissoc(d, "_id")]})
            except JsonSchemaValueException as e:
                identity = f"id {d['id']}" if "id" in d else f"_id {d['_id']}"
                fails.append(f"failed: {name} doc with {identity} ({e})")
    if fails:
        print(f"{len(fails)} fails")
        for f in fails:
            print(f)
        raise Exception("Fails")

    # Note: This explicit `close()` was added because, without it, pytest would display
    #       the following error message after running the test suite (here, some lines
    #       have been omitted and replaced with `...`):
    #       ```
    #       sys:1: ResourceWarning: Unclosed MongoClient opened at:
    #       ...
    #       File "/code/tests/test_data/test_integrity.py", line 18, in test_schema_conformance
    #         mdb = get_mongo(run_config_frozen__normal_env).db
    #       ...
    #       ```
    mdb.client.close()
