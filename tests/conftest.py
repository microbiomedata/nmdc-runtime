from collections import defaultdict
import os
from functools import lru_cache
from typing import Generator, Any

from nmdc_runtime.api.core.util import import_via_dotted_path
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase
import pytest
from _pytest.fixtures import FixtureRequest
from dagster import build_op_context
from dagster._core.execution.context.invocation import DirectOpExecutionContext

from nmdc_runtime.minter.adapters.repository import InMemoryIDStore
from nmdc_runtime.minter.config import (
    typecodes,
    shoulders,
    services,
    requesters,
    schema_classes,
)
from nmdc_runtime.minter.domain.model import MintingRequest, Identifier
from nmdc_runtime.site.ops import materialize_alldocs
from nmdc_runtime.site.resources import mongo_resource, MongoDB
from nmdc_runtime.util import get_class_name_to_collection_names_map, nmdc_schema_view


def minting_request():
    return MintingRequest(
        **{
            "service": services()[0],
            "requester": requesters()[0],
            "schema_class": schema_classes()[0],
            "how_many": 1,
        }
    )


def draft_identifier():
    id_ = "nmdc:bsm-11-z8x8p723"
    return Identifier(
        **{
            "id": id_,
            "name": id_,
            "typecode": {
                "id": next(d["id"] for d in typecodes() if d["name"] == "bsm")
            },
            "shoulder": {"id": next(d["id"] for d in shoulders() if d["name"] == "11")},
            "status": "draft",
        }
    )


@lru_cache
def get_mongo_test_db() -> MongoDatabase:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,
    )
    db: MongoDatabase = _client[os.getenv("MONGO_TEST_DBNAME")]

    for coll_name in [
        "typecodes",
        "shoulders",
        "services",
        "requesters",
        "schema_classes",
    ]:
        db[f"minter.{coll_name}"].drop()
        db[f"minter.{coll_name}"].insert_many(
            import_via_dotted_path(f"nmdc_runtime.minter.config.{coll_name}")()
        )

    return db


@lru_cache()
def get_test_inmemoryidstore() -> InMemoryIDStore:
    return InMemoryIDStore(
        services=services(),
        shoulders=shoulders(),
        typecodes=typecodes(),
        requesters=requesters(),
        schema_classes=schema_classes(),
    )


@pytest.fixture
def client_config():
    return {
        "dbname": os.getenv("MONGO_DBNAME"),
        "host": os.getenv("MONGO_HOST"),
        "password": os.getenv("MONGO_PASSWORD"),
        "username": os.getenv("MONGO_USERNAME"),
    }


@pytest.fixture
def op_context(client_config):
    return build_op_context(
        resources={"mongo": mongo_resource.configured(client_config)}
    )


# A declarative representation -- specifically, a LinkML CollectionInstance
# <https://linkml.io/linkml-model/latest/docs/specification/02instances/#collections> --
# of the `nmdc:Database` constructed in the body of `test_find_data_objects_for_study_having_one`.
_test_sty = "nmdc:sty-11-r2h77870"
_test_bsm = "nmdc:bsm-11-6zd5nb38"
_test_dobj = "nmdc:dobj-11-cpv4y420"
_test_omprc = "nmdc:omprc-11-nmtj1g51"


@pytest.fixture
def docs_1ea_bsm_sty_omprc_wfmgan_dobj():
    return [
        {
            "id": _test_sty,
            "type": "nmdc:Study",
            "study_category": "research_study",
        },
        {
            "id": _test_bsm,
            "type": "nmdc:Biosample",
            "associated_studies": [_test_sty],
            "env_broad_scale": {
                "has_raw_value": "ENVO_00000446",
                "term": {
                    "id": "ENVO:00000446",
                    "name": "terrestrial biome",
                    "type": "nmdc:OntologyClass",
                },
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_local_scale": {
                "has_raw_value": "ENVO_00005801",
                "term": {
                    "id": "ENVO:00005801",
                    "name": "rhizosphere",
                    "type": "nmdc:OntologyClass",
                },
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_medium": {
                "has_raw_value": "ENVO_00001998",
                "term": {
                    "id": "ENVO:00001998",
                    "name": "soil",
                    "type": "nmdc:OntologyClass",
                },
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
        },
        {
            "id": _test_omprc,
            "type": "nmdc:NucleotideSequencing",
            "has_input": [_test_bsm],
            "associated_studies": [_test_sty],
            "analyte_category": "metagenome",
        },
        {
            "id": "nmdc:wfmgan-11-fqq66x60.1",
            "type": "nmdc:MetagenomeAnnotation",
            "has_input": [_test_bsm],
            "has_output": [_test_dobj],
            "was_informed_by": [_test_omprc],
            "started_at_time": "2023-03-24T02:02:59.479107+00:00",
            "ended_at_time": "2023-03-24T02:02:59.479129+00:00",
            "execution_resource": "JGI",
            "git_url": "https://www.example.com",
        },
        {
            "id": _test_dobj,
            "type": "nmdc:DataObject",
            "name": "Raw sequencer read data",
            "description": "Metagenome Raw Reads for nmdc:omprc-11-nmtj1g51",
        },
    ]


@pytest.fixture
def seeded_db(
    request: FixtureRequest, op_context: DirectOpExecutionContext
) -> Generator[MongoDatabase, Any, None]:
    seed_docs = request.getfixturevalue(request.param)
    mongo: MongoDB = op_context.resources.mongo
    class_name_to_collection_names_map = get_class_name_to_collection_names_map(
        nmdc_schema_view()
    )

    # group collection instances by target collection
    docs_by_collection_name = defaultdict(list)
    for doc in seed_docs:
        collection_name = class_name_to_collection_names_map[
            doc["type"].removeprefix("nmdc:")
        ][0]
        docs_by_collection_name[collection_name].append(doc)

    # Seed the db.
    with mongo.client.start_session() as session:
        with session.start_transaction():
            for collection_name, docs in docs_by_collection_name.items():
                mongo.db.get_collection(collection_name).insert_many(
                    docs, session=session
                )
    # XXX nest `materialize_alldocs` in above transaction?
    materialize_alldocs(op_context)

    yield mongo.db

    # 🧹 Clean up.
    with mongo.client.start_session() as session:
        with session.start_transaction():
            for collection_name, docs in docs_by_collection_name.items():
                mongo.db.get_collection(collection_name).delete_many(
                    {"id": {"$in": [d["id"] for d in docs]}}, session=session
                )
    materialize_alldocs(op_context)
