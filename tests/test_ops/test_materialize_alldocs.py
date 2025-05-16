import os
from collections import defaultdict

import pytest
from toolz import assoc, dissoc

from dagster import build_op_context

from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.site.ops import (
    materialize_alldocs,
)
from nmdc_runtime.util import (
    nmdc_schema_view,
    populated_schema_collection_names_with_id_field,
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


def test_materialize_alldocs(op_context):
    mdb = op_context.resources.mongo.db

    # Insert some documents into some upstream collections.
    #
    # Note: This will allow us to look for _specific_ documents in the resulting `alldocs` collection.
    #
    # Note: This collection was chosen mostly arbitrarily. I chose it because I saw that other tests were
    #       not (currently) leaving "residual documents" in it (note: at the time of this writing, the
    #       test database is _not_ being rolled back to a pristine state in between tests).
    #
    # Reference: https://microbiomedata.github.io/berkeley-schema-fy24/FieldResearchSite/#direct
    #
    field_research_site_class_ancestry_chain = [
        "nmdc:FieldResearchSite",
        "nmdc:Site",
        "nmdc:MaterialEntity",
        "nmdc:NamedThing",
    ]
    field_research_site_documents = [
        {
            "id": "frsite-99-00000001",
            "type": "nmdc:FieldResearchSite",
            "name": "Site A",
        },
        {
            "id": "frsite-99-00000002",
            "type": "nmdc:FieldResearchSite",
            "name": "Site B",
        },
        {
            "id": "frsite-99-00000003",
            "type": "nmdc:FieldResearchSite",
            "name": "Site C",
        },
    ]
    field_research_site_set_collection = mdb.get_collection("field_research_site_set")
    for document in field_research_site_documents:
        field_research_site_set_collection.replace_one(document, document, upsert=True)

    # Get a list of non-empty collections in which at least one document has an `id` field.
    #
    # Note: That is the same criteria the function-under-test uses to identify which upstream collections
    #       it will source (i.e. copy) documents from in order to populate the `alldocs` collection.
    #
    collection_names = populated_schema_collection_names_with_id_field(mdb)
    assert "field_research_site_set" in collection_names

    # Invoke the function-under-test.
    #
    # Note: It returns an estimated count; so, we'll just verify that it's an integer,
    #       rather than relying on its value. We'll get an _exact_ count later.
    #
    estimated_number_of_docs_in_alldocs = materialize_alldocs(op_context)
    assert isinstance(estimated_number_of_docs_in_alldocs, int)

    # Get a reference to the newly-materialized `alldocs` collection.
    alldocs_collection = mdb.get_collection("alldocs")
    num_alldocs_docs = alldocs_collection.count_documents(
        {}
    )  # here, we get an _exact_ count

    # Verify each upstream document is represented correctly—and only once—in the `alldocs` collection.
    #
    # Note: We do not check the `type` value here (beyond its data type), due to the current tedium of determining
    #       the class ancestry chain from a dictionary (as opposed to a Python instance). We do check it for some
    #       documents later, but only for documents we inserted above, since we know what to "expect" for those
    #       documents. Here, we just verify that each document's `type` value is of type `array`.
    #
    # Note: We also keep a tally of the number of upstream documents that exist, which we'll reference later.
    #
    num_upstream_docs = 0
    for collection_name in collection_names:
        collection = mdb.get_collection(collection_name)
        for document in collection.find({}):
            num_upstream_docs += 1
            document_having_generic_type = assoc(
                {"id": document["id"]},
                "_type_and_ancestors",
                {"$type": "array"},
            )
            assert alldocs_collection.count_documents(document_having_generic_type) == 1

    # Verify each of the specific documents we created above appears in the `alldocs` collection once,
    # and that `_type_and_ancestors` has been set to its class ancestry chain.
    for document in field_research_site_documents:
        alldocs_document = {
            "id": document["id"],
            "type": document["type"],
            "_type_and_ancestors": field_research_site_class_ancestry_chain,
        }
        assert alldocs_collection.count_documents(alldocs_document) == 1

    # Verify the total number of documents in all the upstream collections, combined,
    # equals the number of documents in the `alldocs` collection.
    assert num_upstream_docs == num_alldocs_docs

    # Clean up: Delete the documents we created within this test, from the database.
    for document in field_research_site_documents:
        field_research_site_set_collection.delete_one(document)
    alldocs_collection.delete_many({})


# A declarative representation -- specifically, a json-serializable `dict`
# (cf. `linkml_runtime.utils.dictutils.as_simple_dict`) -- of the `nmdc:Database` constructed in the body of
# `test_find_data_objects_for_study_having_one`.
_test_nmdc_database_object_bsm_sty_omprc_wfmsa_dobj = {
    "study_set": [
        {
            "id": "nmdc:sty-11-r2h77870",
            "type": "nmdc:Study",
            "study_category": "research_study",
        }
    ],
    "biosample_set": [
        {
            "id": "nmdc:bsm-11-6zd5nb38",
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
            "type": "nmdc:Biosample",
            "associated_studies": ["nmdc:sty-11-r2h77870"],
        }
    ],
    "data_generation_set": [
        {
            "id": "nmdc:omprc-11-nmtj1g51",
            "has_input": ["nmdc:bsm-11-6zd5nb38"],
            "type": "nmdc:NucleotideSequencing",
            "analyte_category": "metagenome",
            "associated_studies": ["nmdc:sty-11-r2h77870"],
        }
    ],
    "data_object_set": [
        {
            "id": "nmdc:dobj-11-cpv4y420",
            "name": "Raw sequencer read data",
            "description": "Metagenome Raw Reads for nmdc:omprc-11-nmtj1g51",
            "type": "nmdc:DataObject",
        }
    ],
    "workflow_execution_set": [
        {
            "id": "nmdc:wfmsa-11-fqq66x60.1",
            "started_at_time": "2023-03-24T02:02:59.479107+00:00",
            "ended_at_time": "2023-03-24T02:02:59.479129+00:00",
            "was_informed_by": "nmdc:omprc-11-nmtj1g51",
            "execution_resource": "JGI",
            "git_url": "https://github.com/microbiomedata/RawSequencingData",
            "has_input": ["nmdc:bsm-11-6zd5nb38"],
            "has_output": ["nmdc:dobj-11-cpv4y420"],
            "type": "nmdc:MetagenomeSequencing",
        }
    ],
}


def test_alldocs_related_ids_with_type_and_ancestors(op_context):
    """
    Test that the {_inbound,_outbound} fields, in conjunction with the _type_and_ancestors field, can be used to find
    all nmdc:DataObjects related to a given nmdc:Biosample using an index-covered query.
    """
    mdb = op_context.resources.mongo.db

    # Prepare to store any existing documents with the IDs we'll be using, to restore later
    existing_docs = defaultdict(list)
    # Prepare to store IDs for each test-document entity by type.
    ids_for = defaultdict(list)

    for (
        collection_name,
        docs,
    ) in _test_nmdc_database_object_bsm_sty_omprc_wfmsa_dobj.items():
        collection = mdb.get_collection(collection_name)
        for doc in docs:

            # Store any existing document
            existing_doc = collection.find_one({"id": doc["id"]})
            if existing_doc:
                existing_docs[collection_name].append(existing_doc)

            # Insert test document
            collection.replace_one({"id": doc["id"]}, doc, upsert=True)

            # Store ID for test document
            ids_for[collection_name].append(doc["id"])

    # Get class ancestry chains via a schema view, ensuring "nmdc:" CURIE prefix.
    schema_view = nmdc_schema_view()
    ancestry_chain = defaultdict(list)
    for cls in {"Biosample", "DataObject"}:
        ancestry_chain[cls] = [
            "nmdc:" + a if not a.startswith("nmdc:") else a
            for a in schema_view.class_ancestors(cls)
        ]

    materialize_alldocs(op_context)

    # Verify that `alldocs` contains our test documents
    alldocs_collection = mdb.get_collection("alldocs")
    for collection_docs in _test_nmdc_database_object_bsm_sty_omprc_wfmsa_dobj.values():
        assert alldocs_collection.count_documents(
            {"id": {"$in": [doc["id"] for doc in collection_docs]}}
        ) == len(collection_docs)

    # Verify that `_outbound` and `_type_and_ancestors` fields are properly set for biosample -> workflow execution.
    biosample_doc = alldocs_collection.find_one({"id": ids_for["biosample_set"][0]})
    assert biosample_doc is not None
    assert "_outbound" in biosample_doc
    assert ids_for["workflow_execution_set"][0] in [
        d["id"] for d in biosample_doc["_outbound"]
    ]
    assert "_type_and_ancestors" in biosample_doc
    assert set(biosample_doc["_type_and_ancestors"]) == set(ancestry_chain["Biosample"])

    # Find the `nmdc:DataObject`(s) related to a `nmdc:Biosample` via a `nmdc:DataEmitterProcess`.
    biosample_id = ids_for["biosample_set"][0]
    related_data_objects = list(
        alldocs_collection.aggregate(
            [
                {"$match": {"id": biosample_id}},
                {
                    "$graphLookup": {
                        "from": "alldocs",
                        "startWith": "$_outbound.id",
                        "connectFromField": "_outbound.id",
                        "connectToField": "id",
                        "as": "influenced",
                    }
                },
                {"$unwind": {"path": "$influenced"}},
                {"$match": {"influenced._type_and_ancestors": "nmdc:DataObject"}},
                {"$replaceRoot": {"newRoot": "$influenced"}},
                {"$unset": ["_id"]},
            ],
            allowDiskUse=True,
        )
    )

    assert ids_for["data_object_set"][0] in [d["id"] for d in related_data_objects]

    # Also test the reverse query - find the `nmdc:Biosample`(s) related to a given `nmdc:DataObject`.
    data_object_id = ids_for["data_object_set"][0]
    related_biosamples = list(
        alldocs_collection.aggregate(
            [
                {"$match": {"id": data_object_id}},
                {
                    "$graphLookup": {
                        "from": "alldocs",
                        "startWith": "$_inbound.id",
                        "connectFromField": "_inbound.id",
                        "connectToField": "id",
                        "as": "was_influenced_by",
                    }
                },
                {"$unwind": {"path": "$was_influenced_by"}},
                {"$match": {"was_influenced_by._type_and_ancestors": "nmdc:Sample"}},
                {"$replaceRoot": {"newRoot": "$was_influenced_by"}},
                {"$unset": ["_id"]},
            ],
            allowDiskUse=True,
        )
    )
    assert len(related_biosamples) == 1
    assert related_biosamples[0]["id"] == biosample_id
    assert related_biosamples[0]["type"] == "nmdc:Biosample"

    # Clean up: Delete the documents we created (if they didn't exist before) or restore them
    for (
        collection_name,
        docs,
    ) in _test_nmdc_database_object_bsm_sty_omprc_wfmsa_dobj.items():
        collection = mdb.get_collection(collection_name)
        for doc in docs:
            # If the document didn't exist before, delete it
            if not any(
                existing_doc["id"] == doc["id"]
                for existing_doc in existing_docs.get(collection_name, [])
            ):
                collection.delete_one({"id": doc["id"]})
            # Otherwise, restore the original document
            else:
                original_doc = next(
                    existing_doc
                    for existing_doc in existing_docs[collection_name]
                    if existing_doc["id"] == doc["id"]
                )
                collection.replace_one({"id": doc["id"]}, original_doc)

    # Re-materalize alldocs
    materialize_alldocs(op_context)
