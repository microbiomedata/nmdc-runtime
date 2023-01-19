import re

from toolz import pluck

from tests.conftest import (
    minting_request,
    SERVICES,
    REQUESTERS,
    SCHEMA_CLASSES,
    TYPECODES,
    draft_identifier,
    SHOULDERS,
)


def test_minting_request():
    mr = minting_request()
    assert mr.service.id in list(pluck("id", SERVICES))
    assert mr.requester.id in list(pluck("id", REQUESTERS))
    assert mr.schema_class.id in list(pluck("id", SCHEMA_CLASSES))
    assert mr.schema_class.id in list(pluck("schema_class", TYPECODES))
    assert mr.how_many > 0


def test_draft_identifier():
    did = draft_identifier()
    assert did.status == "draft"
    assert re.fullmatch(r"nmdc:..-..-.*", did.name)
    assert did.typecode.id in list(pluck("id", TYPECODES))
    assert did.shoulder.id in list(pluck("id", SHOULDERS))
