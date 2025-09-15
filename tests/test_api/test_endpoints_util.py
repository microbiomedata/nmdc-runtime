from pymongo.database import Database
import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import is_num_matching_docs_within_limit
from tests.lib.faker import Faker


@pytest.fixture
def seeded_db_for_filtered_counting():
    db = get_mongo_db()

    # Seed the database.
    faker = Faker()
    studies = faker.generate_studies(10, title="Test Study")
    filter_ = {"title": "Test Study"}
    assert db["study_set"].count_documents(filter_) == 0
    db["study_set"].insert_many(studies)
    assert db["study_set"].count_documents(filter_) == 10

    yield db

    # 🧹 Clean up.
    db["study_set"].delete_many(filter_)


def test_is_num_matching_docs_within_limit(seeded_db_for_filtered_counting: Database):
    # Confirm the database has been seeded the way we expect.
    db = seeded_db_for_filtered_counting
    collection = db["study_set"]
    filter_ = {"title": "Test Study"}
    assert collection.count_documents(filter_) == 10

    # Test: Vary the count.
    with pytest.raises(ValueError):
        assert is_num_matching_docs_within_limit(collection, filter_, -1)
    assert not is_num_matching_docs_within_limit(collection, filter_, 0)
    assert not is_num_matching_docs_within_limit(collection, filter_, 9)
    assert is_num_matching_docs_within_limit(collection, filter_, 10)  # there are exactly 10
    assert is_num_matching_docs_within_limit(collection, filter_, 11)

    # Test: Vary the collection.
    collection = db["empty_collection"]
    assert collection.count_documents({}) == 0
    assert is_num_matching_docs_within_limit(collection, {}, 0)
