import pytest
from nmdc_schema.id_helpers import get_typecode_for_future_ids

from nmdc_runtime.minter import config

# LinkML 1.12 wraps an already-anchored `id` pattern as `^(?:...)$`.
WRAPPED_STUDY_PATTERN = r"^(?:^(nmdc):sty-([0-9][a-z]{0,6}[0-9])-([A-Za-z0-9]{1,})$)$"


@pytest.mark.skipif(
    get_typecode_for_future_ids(WRAPPED_STUDY_PATTERN) is None,
    reason="installed nmdc-schema does not yet read wrapped id patterns (nmdc-schema PR 3511)",
)
def test_typecodes_reads_wrapped_id_patterns(monkeypatch):
    schema_dict = {
        "$defs": {
            "Study": {"properties": {"id": {"pattern": WRAPPED_STUDY_PATTERN}}},
            "ChemicalEntity": {
                "properties": {
                    "id": {
                        "pattern": r"^[a-zA-Z0-9][a-zA-Z0-9_\.]+:[a-zA-Z0-9_][a-zA-Z0-9_\-\/\.,]*$"
                    }
                }
            },
        }
    }
    monkeypatch.setattr(config, "get_nmdc_jsonschema_dict", lambda: schema_dict)
    config.typecodes.cache_clear()
    try:
        assert config.typecodes() == [
            {
                "id": "nmdc:Study_typecode",
                "schema_class": "nmdc:Study",
                "name": "sty",
            }
        ]
    finally:
        config.typecodes.cache_clear()
