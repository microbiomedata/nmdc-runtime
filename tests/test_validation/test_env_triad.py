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
    LOCAL_SCALE_EXTRA_PREFIXES,
    validate_env_triad,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "files"


def _make_term(curie: str, name: str = "test term") -> dict:
    """Build a ControlledIdentifiedTermValue dict for a given CURIE."""
    return {
        "type": "nmdc:ControlledIdentifiedTermValue",
        "term": {"id": curie, "name": name, "type": "nmdc:OntologyClass"},
    }


def _make_biosample(
    bs_id: str = "nmdc:bsm-00-test01",
    broad: str | None = "ENVO:01000253",
    local: str | None = "ENVO:03600095",
    medium: str | None = "ENVO:01001057",
) -> dict:
    """Build a minimal biosample dict with the given env triad CURIEs."""
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
    # A valid PO term for env_local_scale
    "PO:0025034": {
        "id": "PO:0025034",
        "is_obsolete": False,
        "relations": [],
    },
    # A valid UBERON term for env_local_scale
    "UBERON:0000178": {
        "id": "UBERON:0000178",
        "is_obsolete": False,
        "relations": [],
    },
    # An obsolete PO term
    "PO:0000001": {
        "id": "PO:0000001",
        "is_obsolete": True,
        "relations": [],
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

    def __init__(self, docs: dict) -> None:
        self._docs = docs

    def find(self, query: dict, projection: dict | None = None) -> list[dict]:
        """Return docs whose id is in the $in list of the query."""
        curie_list = query.get("id", {}).get("$in", [])
        return [self._docs[c] for c in curie_list if c in self._docs]


class FakeDB:
    """Minimal stand-in for a pymongo Database."""

    def __init__(self, collections: dict) -> None:
        self._collections = collections

    def __getitem__(self, name: str) -> FakeCollection:
        """Return the named collection."""
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


def test_po_term_valid_for_env_local_scale(fake_db):
    bs = _make_biosample(local="PO:0025034")
    result = validate_env_triad([bs], fake_db)
    assert result == {}


def test_uberon_term_valid_for_env_local_scale(fake_db):
    bs = _make_biosample(local="UBERON:0000178")
    result = validate_env_triad([bs], fake_db)
    assert result == {}


def test_obsolete_po_term_rejected(fake_db):
    bs = _make_biosample(local="PO:0000001")
    result = validate_env_triad([bs], fake_db)
    assert any("obsolete" in e for e in result["nmdc:bsm-00-test01"])


def test_unknown_po_term_rejected(fake_db):
    bs = _make_biosample(local="PO:9999999")
    result = validate_env_triad([bs], fake_db)
    assert any("unknown term" in e for e in result["nmdc:bsm-00-test01"])


def test_po_term_not_allowed_for_env_broad_scale(fake_db):
    bs = _make_biosample(broad="PO:0025034")
    result = validate_env_triad([bs], fake_db)
    assert any("not a descendant" in e for e in result["nmdc:bsm-00-test01"])


def test_real_fixture(fake_db):
    fixture_path = FIXTURES_DIR / "nmdc_bsm-12-7mysck21.json"
    with open(fixture_path) as f:
        bs = json.load(f)
    result = validate_env_triad([bs], fake_db)
    assert result == {}
