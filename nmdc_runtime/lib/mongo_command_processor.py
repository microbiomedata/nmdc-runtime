from json import dumps
from logging import getLogger
from typing import Any
import uuid

from bson.json_util import loads
from fastapi import HTTPException, status
from pymongo.database import Database
from refscan.lib.helpers import get_collection_names_from_schema
from refscan.lib.Finder import Finder
from refscan.scanner import identify_referring_documents

from nmdc_runtime.api.core.util import now
from nmdc_runtime.api.models.lib.helpers import derive_delete_specs
from nmdc_runtime.api.models.query import (
    Cmd as MongoCommand,
    CommandResponse as MongoCommandResponse,
    CollStatsCommand,
    CollStatsCommandResponse,
    CountCommand,
    CountCommandResponse,
    DeleteCommand,
    DeleteCommandResponse,
    DeleteSpecs,
)
from nmdc_runtime.util import get_allowed_references, nmdc_schema_view


logger = getLogger(__name__)


class MongoCommandProcessor:
    """
    Note: This class was created as a replacement for the `_run_mdb_cmd` function defined in the
          `nmdc_runtime/api/endpoints/queries.py` file. That function had grown over time to be
          over 400 lines, to have multiple responsibilities, and to accumulate caveats/TODOs.
          This class, initially, shares some of those traits, but we think it'll be easier for
          us to work with over time.
    """

    DELETION_ARCHIVE_DATABASE_NAME = "nmdc_deleted"

    def __init__(self, db: Database):
        """
        Initialize this instance with a `SchemaView` instance and the specified database connection.
        """
        self.db = db
        self.schema_view = nmdc_schema_view()

    @staticmethod
    def _make_mongo_command_document(command: MongoCommand) -> dict[str, Any]:
        """
        Get a MongoDB document representing the specified command.

        References:
        - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
        - https://pymongo.readthedocs.io/en/stable/api/bson/json_util.html

        Note: The implementation of this function was copied from the `_run_mdb_cmd` function
              in the `nmdc_runtime/api/endpoints/queries.py` file.

        >>> from bson import ObjectId
        >>> query = {"_id": ObjectId("000011112222333344445555")}
        >>> command = CountCommand(count="study_set", query=query)
        >>> MongoCommandProcessor._make_mongo_command_document(command)
        {'count': 'study_set', 'query': {'_id': ObjectId('000011112222333344445555')}}
        """

        # Note: `command.model_dump()` converts `ObjectId("...")` into `{"$oid": "..."}`,
        #       so we'll subsequently convert it back (via `bson.json_util.loads` below).
        json_dict: dict = command.model_dump(exclude_unset=True)
        json_string: str = dumps(json_dict)
        mongo_command_document: dict = loads(json_string)
        return mongo_command_document

    @staticmethod
    def _is_response_ok(response: MongoCommandResponse) -> bool:
        """
        Returns `True` if `response.ok` is 1; otherwise, returns `False`.

        >>> response = MongoCommandResponse(ok=0)
        >>> MongoCommandProcessor._is_response_ok(response)
        False
        >>> response = MongoCommandResponse(ok=1)
        >>> MongoCommandProcessor._is_response_ok(response)
        True
        """
        return response.ok == 1

    def _get_nmdc_schema_collection_names(self) -> list[str]:
        """Returns the names of all collections described by the NMDC Schema."""
        return get_collection_names_from_schema(schema_view=self.schema_view)

    def _identify_broken_references_deletion_would_leave_behind(
            self,
            delete_command: DeleteCommand,
            stop_on_first: bool = False,
    ) -> list:
        """
        Identify broken references that would be left behind if Mongo were to run the specified
        "delete" command on the database in its current state.
        
        If `stop_on_first` is `True`, the function will stop checking after it finds a single broken
        reference that would be left behind (this will decrease response times since the function
        won't have to continue looking for additional references).

        Note: This function disregards the "limit" property, if any, of the "delete" specification.
              We have no way of predicting _which_ document Mongo would delete, and so we behave as
              though _all_ matching documents would be deleted.
              TODO: Address the fact that this could create a false negative result when one of the
                    referring documents happens to match the "delete" specification.
        """
        collection = self.db.get_collection(delete_command.delete)
        delete_specs: DeleteSpecs = derive_delete_specs(delete_command=delete_command)

        # Initialize a list of descriptors of the broken references that would be left behind.
        # Note: These descriptors will identify referring _documents_, but not referring _fields_.
        descriptors_of_broken_references: list = []

        # Make a list of the documents that would be deleted.
        target_document_descriptors = list(
            collection.find(
                filter={"$or": [spec["filter"] for spec in delete_specs]},
                projection={"_id": 1, "id": 1, "type": 1},
            )
        )

        # Make a set of their `_id` values (i.e. their ObjectId values).
        target_document_oids = set(
            tdd["_id"] for tdd in target_document_descriptors
        )

        # For each of those documents, check whether it is referenced by any documents that would
        # not be deleted (if so, it means a broken reference would be left behind).
        finder = Finder(database=self.db)
        for target_document_descriptor in target_document_descriptors:
            # If the document descriptor lacks an "id" field, we already know that no documents can
            # reference it (since they would have to _use_ that "id" value to do so). In that case,
            # we won't bother trying to identify referring documents for it.
            if "id" not in target_document_descriptor:
                continue

            # Identify all documents that reference this target document.
            referring_document_descriptors = identify_referring_documents(
                document=target_document_descriptor,  # expects at least "id" and "type"
                schema_view=nmdc_schema_view(),
                references=get_allowed_references(),
                finder=finder,
            )

            # If _any_ referring document is _not_ among those that would be deleted, it means that
            # performing the deletion _would_ leave behind broken references.
            for rdd in referring_document_descriptors:
                source_document_oid = rdd["source_document_object_id"]
                if source_document_oid not in target_document_oids:
                    descriptor_of_broken_reference = dict(
                        source_collection_name=rdd["source_collection_name"],
                        source_class_name=rdd["source_class_name"],
                        source_document_oid=source_document_oid,
                        source_document_id=rdd["source_document_id"],
                        target_document_id=target_document_descriptor["id"],
                    )
                    descriptors_of_broken_references.append(descriptor_of_broken_reference)

                    # If the caller opted to stop after identifying the first reference that would
                    # be broken, stop iterating now.
                    if stop_on_first:
                        break

        return descriptors_of_broken_references

    def _process_count_command(self, command: CountCommand) -> CountCommandResponse:
        """
        Process a MongoDB `count` command.

        >>> from unittest.mock import MagicMock
        >>> mock_db = MagicMock(spec=Database)
        >>> mock_db.command.return_value = {"ok": 1, "n": 123}
        >>> processor = MongoCommandProcessor(mock_db)
        >>> response = processor._process_count_command(CountCommand(count="study_set"))
        >>> response.model_dump()
        {'ok': 1, 'n': 123}
        >>> mock_db.command.assert_called_once_with(command={"count": "study_set"})
        """
        mongo_command_document = self._make_mongo_command_document(command)
        raw_response = self.db.command(command=mongo_command_document)
        return CountCommandResponse(**raw_response)

    def _process_collstats_command(self, command: CollStatsCommand) -> CollStatsCommandResponse:
        """
        Process a MongoDB `collStats` command.

        Note: The `collStats` command has been deprecated since MongoDB 6.2.
              Reference: https://www.mongodb.com/docs/manual/reference/command/collStats/

        >>> from unittest.mock import MagicMock
        >>> mock_db = MagicMock(spec=Database)
        >>> mock_db.command.return_value = {"ok": 1, "ns": "non_existent_collection", "size": 0, "count": 0, "storageSize": 0, "totalIndexSize": 0, "totalSize": 0, "scaleFactor": 1}
        >>> processor = MongoCommandProcessor(mock_db)
        >>> response = processor._process_collstats_command(CollStatsCommand(collStats="non_existent_collection"))
        >>> dumped_response = response.model_dump()
        >>> dumped_response["ok"]
        1
        >>> dumped_response["ns"]
        'non_existent_collection'
        >>> dumped_response["count"]
        0.0
        >>> mock_db.command.assert_called_once_with(command={"collStats": "non_existent_collection"})
        """
        mongo_command_document = self._make_mongo_command_document(command)
        raw_response = self.db.command(command=mongo_command_document)
        return CollStatsCommandResponse(**raw_response)

    def _back_up_documents_before_deletion(self, delete_command: DeleteCommand) -> None:
        """Back up the documents identified by the specified "delete" command."""

        collection = self.db.get_collection(delete_command.delete)
        delete_specs: DeleteSpecs = derive_delete_specs(delete_command=delete_command)

        # Get a cursor referencing the documents that would be deleted.
        target_documents_cursor = collection.find(
            filter={"$or": [spec["filter"] for spec in delete_specs]},
        )

        # Insert each of those documents into the deletion archive database (e.g. "nmdc_deleted").
        deleted_at = now()  # they'll all have the same `deleted_at` timestamp.
        deletion_archive_db = self.db.client.get_database(self.DELETION_ARCHIVE_DATABASE_NAME)
        deletion_archive_collection = deletion_archive_db.get_collection(delete_command.delete)
        documents_to_back_up = []
        for doc in target_documents_cursor:
            documents_to_back_up.append(dict(doc=doc, deleted_at=deleted_at))
        insert_many_result = deletion_archive_collection.insert_many(documents_to_back_up)

        # If we didn't back up all of the documents, raise an exception.
        if len(insert_many_result.inserted_ids) != len(documents_to_back_up):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=(
                    "Deletion failed. We failed to back up the documents before starting deletion. "
                    "The entire operation has been aborted. No documents have been deleted."
                ),
            )

    @staticmethod
    def _generate_user_facing_event_id() -> str:
        r"""
        Get a unique identifier that can be used to correlate a user report with system logs.

        Example: '20260721201139-7bbbc058-45a4-4926-a568-f7b532a4289c'

        Docs: https://docs.python.org/3/library/uuid.html

        >>> import re
        >>> id_ = MongoCommandProcessor._generate_user_facing_event_id()
        >>> re.compile(r"^[0-9]{14}-[0-9a-f\-]{36}$").match(id_) is not None
        True
        >>> id_.startswith("20")
        True
        """
        timestamp: str = now().strftime('%Y%m%d%H%M%S')
        uuid_ = str(uuid.uuid4())
        return f"{timestamp}-{uuid_}"

    def _process_delete_command(
        self,
        command: DeleteCommand,
        allow_broken_refs: bool = False,
    ) -> DeleteCommandResponse:
        """
        Process a MongoDB `delete` command.

        Reference: https://www.mongodb.com/docs/manual/reference/command/delete/
        """

        # If the specified collection isn't described by the schema, raise an exception.
        collection_name = command.delete
        if collection_name not in self._get_nmdc_schema_collection_names():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=f"Collection '{collection_name}' is not described by the NMDC Schema",
            )

        # Check how many documents containing broken references the deletion would leave behind.
        # If there are any, log warnings or raise an exception, depending upon `allow_broken_refs`.
        broken_refs = self._identify_broken_references_deletion_would_leave_behind(
            delete_command=command,
            stop_on_first=True,
        )
        if len(broken_refs) > 0:
            if allow_broken_refs:
                for ref in broken_refs:
                    logger.warning(
                        f"The document having 'id'='{ref['target_document_id']}' in "
                        f"the collection '{ref['collection_name']}' is referenced by "
                        f"the document having 'id'='{ref['source_document_id']}' in "
                        f"the collection '{ref['source_collection_name']}'. "
                        f"Deleting the former will leave behind a broken reference."
                    )
            else:
                # Raise an exception about the first would-be-broken reference.
                #
                # TODO: Consider reporting _all_ would-be-broken references instead of
                #       only the _first_ one we encounter. That would make the response
                #       more informative to the user in cases where there are multiple
                #       such references; but it would also take longer to compute and
                #       would increase the response size (consider the case where the
                #       user-specified filter matches many, many documents).
                ref = broken_refs[0]
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=(
                        f"The operation was not performed, because performing it would "
                        f"have left behind one or more broken references. For example: "
                        f"The document having 'id'='{ref['target_document_id']}' in "
                        f"the collection '{ref['collection_name']}' is referenced by "
                        f"the document having 'id'='{ref['source_document_id']}' in "
                        f"the collection '{ref['source_collection_name']}'. "
                        f"Deleting the former would leave behind a broken reference. "
                        f"Update or delete referring document(s) and try again."
                    ),
                )

        # Back up the would-be deleted documents.
        self._back_up_documents_before_deletion(delete_command=command)

        # Perform the deletion.
        mongo_command_document = self._make_mongo_command_document(command)
        raw_response = self.db.command(command=mongo_command_document)

        # Handle `writeErrors`.
        response = DeleteCommandResponse(**raw_response)
        if isinstance(response.writeErrors, list) and len(response.writeErrors) > 0:
            event_id = self._generate_user_facing_event_id()
            logger.error(f"An error(s) occurred while deleting documents. Event ID: {event_id}")
            logger.error(response.writeErrors)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=(
                    "An error occurred while deleting the specified documents. Please contact "
                    f"an administrator of this application, referencing system event '{event_id}'. "
                ),
            )
    
        return response

    def process(self, command: MongoCommand) -> MongoCommandResponse:
        """
        Submit the specified command to the configured Mongo database.
        """

        # Initialize the response.
        response = MongoCommandResponse(ok=0)

        # Invoke the appropriate method based upon the kind of command.
        if isinstance(command, CountCommand):
            response = self._process_count_command(command)
        elif isinstance(command, CollStatsCommand):
            response = self._process_collstats_command(command)
        elif isinstance(command, DeleteCommand):
            response = self._process_delete_command(command)

            # If no documents were deleted, the user might have made a mistake. In that case,
            # we return an error response as a courtesy.
            if response.n == 0:
                raise HTTPException(
                    status_code=status.HTTP_418_IM_A_TEAPOT,
                    detail="No documents were deleted. Check the syntax of your request."
                )

        # Return the response.
        return response
