import json
from pathlib import Path

import pytest
from fastjsonschema import JsonSchemaValueException

from nmdc_runtime.util import nmdc_jsonschema_validate

REPO_ROOT = Path(__file__).parent.parent


def test_nmdc_jsonschema_validate():
    with open(REPO_ROOT.joinpath("metadata-translation/examples/study_test.json")) as f:
        study_test = json.load(f)
        try:
            _ = nmdc_jsonschema_validate(study_test)
        except JsonSchemaValueException as e:
            pytest.fail(str(e))
