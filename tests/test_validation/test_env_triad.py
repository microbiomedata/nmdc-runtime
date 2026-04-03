"""
Tests for env triad (env_broad_scale, env_local_scale, env_medium) validation.

Run from the repo root:

    pytest -vv tests/test_validation/test_env_triad.py
"""

import json
from pathlib import Path

import pytest

from nmdc_runtime.site.validation.env_triad import (
    ASTRONOMICAL_BODY_PART,
    BIOME,
    ENVIRONMENTAL_MATERIAL,
    validate_env_triad,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "files"


def _make_term(curie, name="test term"):
    return {
        "type": "nmdc:ControlledIdentifiedTermValue",
        "term": {"id": curie, "name": name, "type": "nmdc:OntologyClass"},
    }


def _make_biosample(
    bs_id="nmdc:bsm-00-test01",
    broad="ENVO:01000253",
    local="ENVO:03600095",
    medium="ENVO:01001057",
):
    bs = {"id": bs_id, "type": "nmdc:Biosample"}
    if broad is not None:
        bs["env_broad_scale"] = _make_term(broad)
    if local is not None:
        bs["env_local_scale"] = _make_term(local)
    if medium is not None:
        bs["env_medium"] = _make_term(medium)
    return bs


# -- Fake MongoDB layer (no unittest.mock) --

ONTOLOGY_CLASS_DOCS = {
    "ENVO:01000253": {
        "id": "ENVO:01000253",
        "is_obsolete": False,
        "relations": [
            {"predicate": "entailed_isa_partof_closure", "object": BIOME},
        ],
    },
    "ENVO:03600095": {
        "id": "ENVO:03600095",
        "is_obsolete": False,
        "relations": [
            {
                "predicate": "entailed_isa_partof_closure",
                "object": ASTRONOMICAL_BODY_PART,
            },
        ],
    },
    "ENVO:01001057": {
        "id": "ENVO:01001057",
        "is_obsolete": False,
        "relations": [
            {
                "predicate": "entailed_isa_partof_closure",
                "object": ENVIRONMENTAL_MATERIAL,
            },
        ],
    },
    "ENVO:99999999": {
        "id": "ENVO:99999999",
        "is_obsolete": True,
        "relations": [
            {"predicate": "entailed_isa_partof_closure", "object": BIOME},
        ],
    },
    # A biome-typed term — invalid for env_medium / env_local_scale
    "ENVO:00000446": {
        "id": "ENVO:00000446",
        "is_obsolete": False,
        "relations": [
            {"predicate": "entailed_isa_partof_closure", "object": BIOME},
        ],
    },
}


class FakeCollection:
    """Minimal stand-in for a pymongo Collection that serves canned docs."""

    def __init__(self, docs):
        self._docs = docs

    def find(self, query, projection=None):
        curie_list = query.get("id", {}).get("$in", [])
        return [self._docs[c] for c in curie_list if c in self._docs]


class FakeDB:
    """Minimal stand-in for a pymongo Database."""

    def __init__(self, collections):
        self._collections = collections

    def __getitem__(self, name):
        return self._collections[name]


@pytest.fixture
def fake_db():
    return FakeDB({"ontology_class_set": FakeCollection(ONTOLOGY_CLASS_DOCS)})


def test_valid_biosample(fake_db):
    result = validate_env_triad([_make_biosample()], fake_db)
    assert result == {}


def test_multiple_valid_biosamples(fake_db):
    result = validate_env_triad(
        [_make_biosample(bs_id=f"nmdc:bsm-00-test{i:02d}") for i in range(3)],
        fake_db,
    )
    assert result == {}


def test_missing_field(fake_db):
    bs = _make_biosample(broad=None)
    result = validate_env_triad([bs], fake_db)
    assert any("Missing required field" in e for e in result["nmdc:bsm-00-test01"])


def test_missing_term_id(fake_db):
    bs = _make_biosample()
    bs["env_broad_scale"] = {"type": "nmdc:ControlledIdentifiedTermValue"}
    result = validate_env_triad([bs], fake_db)
    assert any("missing term.id" in e for e in result["nmdc:bsm-00-test01"])


def test_unknown_term(fake_db):
    bs = _make_biosample(broad="ENVO:00000000")
    result = validate_env_triad([bs], fake_db)
    assert any("unknown term" in e for e in result["nmdc:bsm-00-test01"])


def test_obsolete_term(fake_db):
    bs = _make_biosample(broad="ENVO:99999999")
    result = validate_env_triad([bs], fake_db)
    assert any("obsolete" in e for e in result["nmdc:bsm-00-test01"])


def test_wrong_hierarchy_broad_scale(fake_db):
    bs = _make_biosample(broad="ENVO:01001057")
    result = validate_env_triad([bs], fake_db)
    assert any("not a descendant" in e for e in result["nmdc:bsm-00-test01"])


def test_wrong_hierarchy_env_medium(fake_db):
    bs = _make_biosample(medium="ENVO:01000253")
    result = validate_env_triad([bs], fake_db)
    assert any("not a descendant" in e for e in result["nmdc:bsm-00-test01"])


def test_biome_disallowed_in_env_medium(fake_db):
    bs = _make_biosample(medium="ENVO:00000446")
    result = validate_env_triad([bs], fake_db)
    assert any("must not be a descendant" in e for e in result["nmdc:bsm-00-test01"])


def test_biome_disallowed_in_env_local_scale(fake_db):
    bs = _make_biosample(local="ENVO:00000446")
    result = validate_env_triad([bs], fake_db)
    assert any("must not be a descendant" in e for e in result["nmdc:bsm-00-test01"])


def test_duplicate_curie_across_fields(fake_db):
    bs = _make_biosample(broad="ENVO:01000253", local="ENVO:01000253")
    result = validate_env_triad([bs], fake_db)
    assert any("already used" in e for e in result["nmdc:bsm-00-test01"])


def test_empty_biosamples_list(fake_db):
    result = validate_env_triad([], fake_db)
    assert result == {}


def test_real_fixture(fake_db):
    fixture_path = FIXTURES_DIR / "nmdc_bsm-12-7mysck21.json"
    with open(fixture_path) as f:
        bs = json.load(f)
    result = validate_env_triad([bs], fake_db)
    assert result == {}
