from uuid import uuid4

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.query import (
    CollStatsCommand,
    CollStatsCommandResponse,
    CountCommand,
    CountCommandResponse,
    DeleteCommand,
    DeleteCommandResponse,
    DeleteStatement,
)
from nmdc_runtime.lib.mongo_command_processor import MongoCommandProcessor
from tests.lib.faker import Faker


class TestMongoCommandProcessor:
    def test_it_backs_up_documents_before_deleting_them(self):
        """Confirms that the documents that get backed up are the same ones that get deleted."""

        db = get_mongo_db()
        study_set = db.get_collection("study_set")
        deleted_study_set = db.client["nmdc_deleted"].get_collection("study_set")

        # Seed the database with 4 studies: 2 of which we will delete, and 2 of which we will spare.
        faker = Faker()
        unique_str = str(uuid4())
        title_of_studies_to_delete = f"Doomed study from test {unique_str}"
        title_of_studies_to_spare = f"Spared study from test {unique_str}"
        studies_to_delete = faker.generate_studies(2, title=title_of_studies_to_delete)
        studies_to_spare = faker.generate_studies(2, title=title_of_studies_to_spare)
        assert study_set.count_documents({"title": title_of_studies_to_delete}) == 0
        assert study_set.count_documents({"title": title_of_studies_to_spare}) == 0
        studies_to_delete_oids = study_set.insert_many(studies_to_delete).inserted_ids
        studies_to_spare_oids = study_set.insert_many(studies_to_spare).inserted_ids
        inserted_oids = studies_to_delete_oids + studies_to_spare_oids
        assert study_set.count_documents({"title": title_of_studies_to_delete}) == 2
        assert study_set.count_documents({"title": title_of_studies_to_spare}) == 2

        try:
            # Submit the command for processing.
            response = MongoCommandProcessor(db=db).process(
                command=DeleteCommand(
                    delete="study_set",
                    deletes=[
                        DeleteStatement(
                            q={"title": title_of_studies_to_delete}, limit=0
                        )
                    ],
                ),
            )
            assert isinstance(response, DeleteCommandResponse)
            assert response.n == 2

            # Confirm the studies that were backed up were the ones that were deleted.
            assert study_set.count_documents({"title": title_of_studies_to_delete}) == 0
            assert (
                deleted_study_set.count_documents(
                    {"doc._id": {"$in": studies_to_delete_oids}}
                )
                == 2
            )
            for deleted_study in studies_to_delete:
                deleted_study_oid = deleted_study["_id"]
                backed_up_study = deleted_study_set.find_one(
                    {"doc._id": deleted_study_oid}
                )["doc"]
                assert deleted_study == backed_up_study

            # Also, confirm the studies that were not deleted were not backed up.
            assert study_set.count_documents({"title": title_of_studies_to_spare}) == 2
            assert (
                deleted_study_set.count_documents(
                    {"doc._id": {"$in": studies_to_spare_oids}}
                )
                == 0
            )
        finally:
            # Clean up both databases.
            study_set.delete_many({"_id": {"$in": inserted_oids}})
            deleted_study_set.delete_many({"doc._id": {"$in": inserted_oids}})

    def test_it_processes_count_commands(self):
        """Confirms the processor can count documents matching a query."""

        # Seed the database with a couple studies that we can count.
        db = get_mongo_db()
        study_set = db.get_collection("study_set")
        study_title = f"CountCommand test {uuid4()}"
        studies = Faker().generate_studies(2, title=study_title)
        assert study_set.count_documents({"title": study_title}) == 0
        inserted_oids = study_set.insert_many(studies).inserted_ids
        assert study_set.count_documents({"title": study_title}) == 2

        try:
            response = MongoCommandProcessor(db=db).process(
                command=CountCommand(
                    count="study_set",
                    query={"title": study_title},
                )
            )

            assert isinstance(response, CountCommandResponse)
            assert response.ok == 1
            assert response.n == 2
        finally:
            # Clean up the database.
            study_set.delete_many({"_id": {"$in": inserted_oids}})

    def test_it_processes_collstats_commands(self):
        """Confirms the processor can retrieve statistics for a collection."""

        db = get_mongo_db()
        response = MongoCommandProcessor(db=db).process(
            command=CollStatsCommand(collStats="study_set")
        )

        assert isinstance(response, CollStatsCommandResponse)
        assert response.ok == 1
        assert response.ns == f"{db.name}.study_set"
        assert response.count >= 0
