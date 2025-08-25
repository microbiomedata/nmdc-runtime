from pymongo.database import Database
import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import does_num_matching_docs_exceed_threshold
from tests.lib.faker import Faker


@pytest.fixture
def seeded_db_for_filtered_counting():
    db = get_mongo_db()

    # Seed the database.
    faker = Faker()
    studies = faker.generate_studies(10, title="Test Study")
    studies[0]["title"] = "Test Study (outlier)"  # one different title
    filter_ = {"title": {"$regex": "^Test Study"}}
    assert db["study_set"].count_documents(filter_) == 0
    db["study_set"].insert_many(studies)
    assert db["study_set"].count_documents(filter_) == 10

    yield db

    # 🧹 Clean up.
    db["study_set"].delete_many(filter_)


def test_does_num_matching_docs_exceed_threshold(seeded_db_for_filtered_counting: Database):
    # Seed the database.
    db = seeded_db_for_filtered_counting
    collection = db["study_set"]
    filter_a = {"title": "Test Study"}
    filter_b = {"title": "Test Study (outlier)"}
    filter_c = {"title": {"$regex": "^Test Study"}}
    filter_d = {"title": "Nonexistent Study"}
    assert collection.count_documents(filter_a) == 9
    assert collection.count_documents(filter_b) == 1
    assert collection.count_documents(filter_c) == 10
    assert collection.count_documents(filter_d) == 0

    # Test: Vary the count.
    with pytest.raises(ValueError):
        assert does_num_matching_docs_exceed_threshold(collection, filter_a, -1)
    assert does_num_matching_docs_exceed_threshold(collection, filter_a, 0)
    assert not does_num_matching_docs_exceed_threshold(collection, filter_a, 9)  # there are exactly 9
    assert not does_num_matching_docs_exceed_threshold(collection, filter_a, 10)
    assert not does_num_matching_docs_exceed_threshold(collection, filter_a, 11)

    # Test: Vary the filter.
    assert does_num_matching_docs_exceed_threshold(collection, filter_b, 0)
    assert not does_num_matching_docs_exceed_threshold(collection, filter_b, 1)  # there is exactly 1
    assert not does_num_matching_docs_exceed_threshold(collection, filter_b, 2)
    assert does_num_matching_docs_exceed_threshold(collection, filter_c, 9)
    assert not does_num_matching_docs_exceed_threshold(collection, filter_c, 10)  # there are exactly 10
    assert not does_num_matching_docs_exceed_threshold(collection, filter_c, 11)
    assert not does_num_matching_docs_exceed_threshold(collection, filter_d, 0)  # there are exactly 0
    assert not does_num_matching_docs_exceed_threshold(collection, filter_d, 1)

    # Test: Vary collection.
    collection = db["empty_collection"]
    assert collection.count_documents({}) == 0
    assert not does_num_matching_docs_exceed_threshold(collection, {}, 0)
