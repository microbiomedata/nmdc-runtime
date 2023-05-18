import fastjsonschema
import pytest
from fastjsonschema import JsonSchemaValueException
from toolz import dissoc

from nmdc_runtime.api.db.mongo import nmdc_schema_collection_names
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import get_nmdc_jsonschema_dict


@pytest.mark.skip(reason="no data tests for code CI")
def test_schema_conformance():
    mdb = get_mongo(run_config_frozen__normal_env).db
    names = nmdc_schema_collection_names(mdb)
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
