import re

from toolz import pluck

from nmdc_runtime.minter.config import (
    services,
    requesters,
    schema_classes,
    typecodes,
    shoulders,
)
from tests.conftest import (
    minting_request,
    draft_identifier,
)


def test_minting_request():
    mr = minting_request()
    assert mr.service.id in list(pluck("id", services()))
    assert mr.requester.id in list(pluck("id", requesters()))
    assert mr.schema_class.id in list(pluck("id", schema_classes()))
    assert mr.schema_class.id in list(pluck("schema_class", typecodes()))
    assert mr.how_many > 0


def test_draft_identifier():
    did = draft_identifier()
    assert did.status == "draft"
    assert re.fullmatch(r"nmdc:[a-z]{2,6}-..-.*", did.name)
    assert did.typecode.id in list(pluck("id", typecodes()))
    assert did.shoulder.id in list(pluck("id", shoulders()))
