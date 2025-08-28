import os

import pytest
from _pytest.fixtures import FixtureRequest
from toolz import assoc

from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.endpoints.lib.linked_instances import (
    pipeline_for_instances_linked_to_ids_by_direction,
)
from nmdc_runtime.site.ops import (
    materialize_alldocs,
)
from nmdc_runtime.util import (
    nmdc_schema_view,
    populated_schema_collection_names_with_id_field,
)

from tests.conftest import docs_1ea_bsm_sty_omprc_wfmgan_dobj


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


@pytest.mark.parametrize(
    "seeded_db", ["docs_1ea_bsm_sty_omprc_wfmgan_dobj"], indirect=True
)
def test_alldocs_linked_instances_with_type_and_ancestors(
    docs_1ea_bsm_sty_omprc_wfmgan_dobj,
    seeded_db: MongoDatabase,
):
    """
    Test that the {_upstream,_downstream} fields, in conjunction with the _type_and_ancestors field, can be used to find
    all nmdc:DataObjects linked to a given nmdc:Biosample, and all nmdc:Biosamples linked to a nmdc:DataObject.
    """
    # Verify that `alldocs` contains our test documents
    alldocs_collection = seeded_db.get_collection("alldocs")
    seeded_docs = docs_1ea_bsm_sty_omprc_wfmgan_dobj
    assert alldocs_collection.count_documents(
        {"id": {"$in": [doc["id"] for doc in seeded_docs]}}
    ) == len(seeded_docs)

    def id_for_seeded_doc_of_class(cls: str) -> str:
        assert not cls.startswith("nmdc:")
        class_descendants = nmdc_schema_view().class_descendants(cls)
        return next(
            d["id"]
            for d in seeded_docs
            if d["type"].removeprefix("nmdc:") in class_descendants
        )

    # Verify that {`_type_and_ancestors`,`_downstream`} are properly set for Biosample -> WorkflowExecution.
    seeded_biosample_id = id_for_seeded_doc_of_class("Biosample")
    alldocs_biosample_doc = alldocs_collection.find_one({"id": seeded_biosample_id})
    assert alldocs_biosample_doc is not None
    assert "_type_and_ancestors" in alldocs_biosample_doc
    assert set(alldocs_biosample_doc["_type_and_ancestors"]) == set(
        f"nmdc:{a}" for a in nmdc_schema_view().class_ancestors("Biosample")
    )
    assert "_downstream" in alldocs_biosample_doc
    assert id_for_seeded_doc_of_class("WorkflowExecution") in [
        d["id"] for d in alldocs_biosample_doc["_downstream"]
    ]

    # Find the `nmdc:DataObject`(s) linked to a `nmdc:Biosample` via a `nmdc:DataEmitterProcess`.
    linked_data_objects = list(
        alldocs_collection.aggregate(
            pipeline_for_instances_linked_to_ids_by_direction(
                ids=[seeded_biosample_id],
                types=["nmdc:DataObject"],
                direction="downstream",
            ),
            allowDiskUse=True,
        )
    )
    print(f"{linked_data_objects=}")
    seeded_data_object_id = id_for_seeded_doc_of_class("DataObject")
    assert seeded_data_object_id in [d["id"] for d in linked_data_objects]

    # Also test the reverse query - find the `nmdc:Biosample`(s) linked to a given `nmdc:DataObject`.
    linked_biosamples = list(
        alldocs_collection.aggregate(
            pipeline_for_instances_linked_to_ids_by_direction(
                ids=[seeded_data_object_id],
                direction="upstream",
                types=["nmdc:Biosample"],
            ),
            allowDiskUse=True,
        )
    )
    assert len(linked_biosamples) == 1
    assert linked_biosamples[0]["id"] == seeded_biosample_id
    assert linked_biosamples[0]["type"] == "nmdc:Biosample"
