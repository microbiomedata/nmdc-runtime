import collections

from nmdc_runtime.minter.adapters.repository import InMemoryIDStore, MongoIDStore
from nmdc_runtime.minter.domain.model import (
    ResolutionRequest,
    Identifier,
    DeleteRequest,
)
from tests.conftest import minting_request, get_mongo_test_db, get_test_inmemoryidstore


def test_mint_one():
    s: InMemoryIDStore = get_test_inmemoryidstore()
    req_mint = minting_request()
    assert req_mint.how_many == 1
    ids = s.mint(req_mint)
    assert len(ids) == 1


def test_mint_many():
    s: InMemoryIDStore = get_test_inmemoryidstore()
    req_mint = minting_request()
    req_mint.how_many = 1_000
    ids = s.mint(req_mint)
    assert len(ids) == 1_000


def test_mint_and_resolve():
    s: InMemoryIDStore = get_test_inmemoryidstore()
    req_mint = minting_request()
    id_: Identifier = next(i for i in s.mint(req_mint))
    req_res = ResolutionRequest(id_name=id_.name, **req_mint.dict())
    assert s.resolve(req_res) is not None


def test_mint_and_delete():
    s: InMemoryIDStore = get_test_inmemoryidstore()
    req_mint = minting_request()
    id_: Identifier = next(i for i in s.mint(req_mint))
    req_del = DeleteRequest(id_name=id_.name, **req_mint.dict())
    s.delete(req_del)
    assert s.resolve(ResolutionRequest(**req_del.dict())) is None


def test_mongo_mint_one():
    s = MongoIDStore(get_mongo_test_db())
    req_mint = minting_request()
    assert req_mint.how_many == 1
    s.db.id_records.drop()

    ids = s.mint(req_mint)
    assert len(ids) == 1
    assert s.db.id_records.count_documents({}) == 1


def test_mongo_mint_many():
    s = MongoIDStore(get_mongo_test_db())
    req_mint = minting_request()
    s.db.id_records.drop()

    req_mint.how_many = 1_000
    ids = s.mint(req_mint)
    assert len(ids) == 1_000
    assert s.db.id_records.count_documents({}) == 1_000


def test_mongo_mint_and_resolve():
    s = MongoIDStore(get_mongo_test_db())
    req_mint = minting_request()
    s.db.id_records.drop()

    id_: Identifier = next(i for i in s.mint(req_mint))
    req_res = ResolutionRequest(id_name=id_.name, **req_mint.dict())
    assert s.resolve(req_res) is not None


def test_mongo_mint_and_delete():
    s = MongoIDStore(get_mongo_test_db())
    req_mint = minting_request()
    s.db.id_records.drop()

    id_: Identifier = next(i for i in s.mint(req_mint))
    req_del = DeleteRequest(id_name=id_.name, **req_mint.dict())
    s.delete(req_del)
    assert s.resolve(ResolutionRequest(**req_del.dict())) is None
    assert s.db.id_records.count_documents({}) == 0
