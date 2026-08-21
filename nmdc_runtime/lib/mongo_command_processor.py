from json import dumps
from logging import getLogger
from typing import Any, Iterator, List
import uuid

from bson import ObjectId
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
    DeleteStatement,
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
    NUM_DOCUMENTS_PER_DELETION_BATCH = 10_000

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

    def _generate_batches[T](
        self, items: list[T], batch_size: int | None = None,
    ) -> Iterator[list[T]]:
        """
        Yields batches of the specified items, of the specified size. If no size is specified,
        uses the value of `self.NUM_DOCUMENTS_PER_DELETION_BATCH` as the batch size.

        >>> list(self._generate_batches([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        >>> list(self._generate_batches(["a", "b", "c"], 2))
        [['a', 'b'], ['c']]

        >>> list(self._generate_batches([], 2))
        []
        """
        if not isinstance(batch_size, int):
            batch_size = self.NUM_DOCUMENTS_PER_DELETION_BATCH
        for batch_start in range(0, len(items), batch_size):
            yield items[batch_start : batch_start + batch_size]

    def _get_target_document_oids_for_deletion(
        self, delete_command: DeleteCommand
    ) -> List[ObjectId]:
        """
        Returns a sorted list of the `ObjectId`s (i.e. `_id` values) of the documents matching a
        given command, considering all of the command's constituent specifications.

        For commands containing multiple specifications, documents selected by earlier
        specifications are excluded from later specifications. This is to simulate the way MongoDB
        processes sequences of specifications (i.e. MongoDB deletes the documents matching earlier
        specifications before processing later specifications).

        >>> # Seed the mock database:
        >>> from mongomock import MongoClient
        >>> db = MongoClient().nmdc
        >>> _ = db.food_set.insert_many([
        ...     {"_id": ObjectId("000000000000000000000001"), "name": "apple"},
        ...     {"_id": ObjectId("000000000000000000000002"), "name": "banana"},
        ...     {"_id": ObjectId("000000000000000000000003"), "name": "carrot"},
        ...     {"_id": ObjectId("000000000000000000000004"), "name": "daikon"},
        ... ])

        1. The result is de-duplicated.
        >>> command = DeleteCommand(delete="food_set", deletes=[
        ...     {"q": {"name": {"$in": ["apple", "banana"]}}, "limit": 0},  # 1 and 2
        ...     {"q": {"name": "apple"}, "limit": 0},                       # 1 again
        ...     {"q": {"name": "daikon"}, "limit": 0},                      # 4
        ... ])
        >>> MongoCommandProcessor(db)._get_target_document_oids_for_deletion(command)
        [ObjectId('000000000000000000000001'), ObjectId('000000000000000000000002'), ObjectId('000000000000000000000004')]

        2. The "limit" gets applied, but we don't know which document Mongo will pick.
        >>> command = DeleteCommand(delete="food_set", deletes=[
        ...     {"q": {"name": {"$in": ["apple", "banana"]}}, "limit": 1},  # 1 or 2, we don't know
        ... ])
        >>> oids = MongoCommandProcessor(db)._get_target_document_oids_for_deletion(command)
        >>> len(oids)
        1
        >>> any([ObjectId('000000000000000000000001') in oids, ObjectId('000000000000000000000002') in oids])
        True

        3. Specifications are processed sequentially (we leverage "limit" for this demonstration).
        >>> command = DeleteCommand(delete="food_set", deletes=[
        ...     {"q": {"name": {"$in": ["apple", "banana"]}}, "limit": 1},
        ...     {"q": {"name": {"$in": ["apple", "banana"]}}, "limit": 1},
        ... ])
        >>> MongoCommandProcessor(db)._get_target_document_oids_for_deletion(command)
        [ObjectId('000000000000000000000001'), ObjectId('000000000000000000000002')]
        """
        document_oids: set[ObjectId] = set()

        collection = self.db.get_collection(delete_command.delete)
        delete_specs: DeleteSpecs = derive_delete_specs(delete_command=delete_command)
        for spec in delete_specs:
            specified_documents_cursor = collection.find(
                filter=spec["filter"],
                projection={"_id": 1},
                hint=spec["hint"] if "hint" in spec else None,
                limit=0,  # we apply the limit, if any, manually below
            )
            for document in specified_documents_cursor:
                document_oid = document["_id"]

                # If we've already dealt with this document, skip it.
                # This simulates MongoDB having deleted it already.
                if document_oid not in document_oids:
                    document_oids.add(document_oid)
                    if spec["limit"] == 1:
                        break

        return sorted(document_oids, key=str)

    def _identify_broken_references_deletion_would_leave_behind(
        self,
        collection_name: str,
        target_document_oids: list[ObjectId],
        stop_on_first: bool = False,
    ) -> list:
        """
        Identify documents that would remain after the deletion, which contain references to any of
        the documents that would be deleted. The `ObjectId`s of the documents that would be deleted
        are passed to this function via the `target_document_oids` parameter.

        If `stop_on_first` is `True`, the function will stop checking after it finds a single such
        document that would be left behind (this will decrease return times since the function won't
        have to continue looking for additional such documents).
        """
        collection = self.db.get_collection(collection_name)

        # Initialize a list of descriptors of the broken references that would be left behind.
        # Note: These descriptors will identify referring _documents_, but not referring _fields_.
        descriptors_of_broken_references: list = []

        # Get the `id` and `type` values of the documents that would be deleted.
        target_document_descriptors = list(
            collection.find(
                filter={"_id": {"$in": target_document_oids}},
                projection={"_id": 1, "id": 1, "type": 1},
            )
        )

        # Make a set, so existence searches are faster (than with a list).
        target_document_oids_set: set = set(target_document_oids)

        # For each of those documents, check whether it is referenced by any documents that would
        # _not_ be deleted (if so, it means a broken reference would be left behind).
        is_scan_aborted = False
        finder = Finder(database=self.db)
        for target_document_descriptor in target_document_descriptors:
            # If the "is scan aborted" flag has been set, break out of this loop. This happens when
            # the `stop_on_first` flag is set and we have already found our first violation.
            if is_scan_aborted:
                break

            # If the document descriptor lacks an "id" field, we already know that no documents can
            # reference it (since they would have to _use_ that "id" value to do so). In that case,
            # we won't bother trying to identify documents that reference it.
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
            # performing the deletion _would_ leave behind a referring document (i.e. a broken reference).
            for rdd in referring_document_descriptors:
                source_document_oid = rdd["source_document_object_id"]
                source_collection_name = rdd["source_collection_name"]
                if not (
                    source_collection_name == collection_name
                    and source_document_oid in target_document_oids_set
                ):
                    descriptor_of_broken_reference = dict(
                        source_collection_name=rdd["source_collection_name"],
                        source_class_name=rdd["source_class_name"],
                        source_document_oid=source_document_oid,
                        source_document_id=rdd["source_document_id"],
                        target_document_id=target_document_descriptor["id"],
                    )
                    descriptors_of_broken_references.append(
                        descriptor_of_broken_reference
                    )

                    # If the caller opted to stop after identifying the first reference that would
                    # be broken, stop iterating now.
                    if stop_on_first:
                        is_scan_aborted = True
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

    def _process_collstats_command(
        self, command: CollStatsCommand
    ) -> CollStatsCommandResponse:
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

    def _back_up_documents_before_deletion(
        self,
        collection_name: str,
        target_document_oids: list[ObjectId],
    ) -> None:
        """Back up the documents identified by the specified "delete" command."""

        # If no `ObjectId`s were specified, return early.
        if len(target_document_oids) == 0:
            logger.debug("No documents were specified to be backed up.")
            return None

        # Get a cursor referencing the documents that would be deleted.
        collection = self.db.get_collection(collection_name)
        target_documents_cursor = collection.find(
            filter={"_id": {"$in": target_document_oids}},
        )

        # Insert each of those documents into the deletion archive database (e.g. "nmdc_deleted").
        deleted_at = now()  # they'll all have the same `deleted_at` timestamp.
        deletion_archive_db = self.db.client.get_database(
            self.DELETION_ARCHIVE_DATABASE_NAME
        )
        deletion_archive_collection = deletion_archive_db.get_collection(
            collection_name
        )
        documents_to_back_up = []
        for target_document in target_documents_cursor:
            documents_to_back_up.append(
                dict(doc=target_document, deleted_at=deleted_at)
            )

        if len(documents_to_back_up) < len(target_document_oids):
            logger.warning(
                f"We expected to back up {len(target_document_oids)} documents, "
                f"but we found only {len(documents_to_back_up)} documents to back up."
            )

        if len(documents_to_back_up) > 0:
            insert_many_result = deletion_archive_collection.insert_many(
                documents_to_back_up
            )

            # If we didn't back up all of the documents we found, raise an exception.
            # TODO: Consider delegating the reporting that "no documents have been deleted" to the
            #       caller, since the actual deletion is not one of this method's concerns.
            if len(insert_many_result.inserted_ids) != len(documents_to_back_up):
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=(
                        "We failed to back up some documents before starting the deletion. "
                        "The entire operation has been aborted. No documents have been deleted."
                    ),
                )
        else:
            logger.debug("There are no documents to back up.")

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
        timestamp: str = now().strftime("%Y%m%d%H%M%S")
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

        # Get the `_id` values of the documents that would be deleted.
        target_document_oids = self._get_target_document_oids_for_deletion(command)

        # Determine whether any documents that reference those documents would be left behind
        # if the deletion were to be performed.
        referrers_left_behind = (
            self._identify_broken_references_deletion_would_leave_behind(
                collection_name=collection_name,
                target_document_oids=target_document_oids,
                stop_on_first=True,
            )
        )

        # Check how many documents containing broken references the deletion would leave behind.
        # If there are any, log warnings or raise an exception, depending upon `allow_broken_refs`.
        if len(referrers_left_behind) > 0:
            if allow_broken_refs:
                for r in referrers_left_behind:
                    logger.warning(
                        f"The document having 'id'='{r['target_document_id']}' in "
                        f"the collection '{collection_name}' is referenced by "
                        f"the document having 'id'='{r['source_document_id']}' in "
                        f"the collection '{r['source_collection_name']}'. "
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
                r = referrers_left_behind[0]
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=(
                        f"The operation was not performed, because performing it would "
                        f"have left behind one or more broken references. For example: "
                        f"The document having 'id'='{r['target_document_id']}' in "
                        f"the collection '{collection_name}' is referenced by "
                        f"the document having 'id'='{r['source_document_id']}' in "
                        f"the collection '{r['source_collection_name']}'. "
                        f"Deleting the former would leave behind a broken reference. "
                        f"Update or delete referring document(s) and try again."
                    ),
                )

        # Back up the would-be deleted documents.
        self._back_up_documents_before_deletion(
            collection_name=collection_name,
            target_document_oids=target_document_oids,
        )

        # Delete the documents, in batches.
        #
        # Note: We use batches (and issue one "delete" command per batch) instead of using a
        #       single "delete" command for everything; because, with enough `_id` values, the size
        #       of the latter _command_ document could approach MongoDB's 16 MiB document size limit,
        #       which would cause the deletion to fail. We use batches to _limit_ the size of any
        #       given _command_ document.
        #       Docs: https://www.mongodb.com/docs/manual/core/document/#document-size-limit
        #
        num_documents_deleted_total = 0
        for target_document_oids_in_batch in self._generate_batches(
            items=target_document_oids,
            batch_size=self.NUM_DOCUMENTS_PER_DELETION_BATCH,
        ):
            delete_command = DeleteCommand(
                delete=collection_name,
                deletes=[
                    DeleteStatement(
                        q={"_id": {"$in": target_document_oids_in_batch}}, limit=0
                    ),
                ],
            )
            mongo_command_document = self._make_mongo_command_document(delete_command)
            raw_response = self.db.command(
                command=mongo_command_document,
                comment="Deletion of document batch by MongoCommandProcessor within nmdc-runtime",
            )

            # Handle `writeErrors` from this batch.
            response = DeleteCommandResponse(**raw_response)
            if isinstance(response.writeErrors, list) and len(response.writeErrors) > 0:
                event_id = self._generate_user_facing_event_id()
                logger.error(
                    f"An error(s) occurred while deleting a batch of documents. Event ID: {event_id}"
                )
                logger.error(response.writeErrors)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=(
                        "An error occurred while deleting the specified documents. Please contact "
                        f"an administrator of this application, referencing system event '{event_id}'. "
                    ),
                )

            # Update the total number of documents that have been deleted.
            num_documents_deleted_in_batch = response.n
            num_documents_deleted_total += num_documents_deleted_in_batch

            # If the deletion of this batch failed from MongoDB's perspective, stop processing the
            # overall "delete" command and return the total number deleted so far.
            # Docs: https://www.mongodb.com/docs/manual/tutorial/use-database-commands/#command-responses
            if not self._is_response_ok(response):
                return DeleteCommandResponse(ok=0, n=num_documents_deleted_total)

        return DeleteCommandResponse(ok=1, n=num_documents_deleted_total)

    def process(
        self,
        command: MongoCommand,
        allow_broken_refs: bool = False,
    ) -> MongoCommandResponse:
        """
        Submit the specified command to the configured Mongo database.

        For "delete" commands, the `allow_broken_refs` parameter controls whether this processor
        will still carry out the deletion, even if it would leave behind broken reference. This
        is offered as an option because a "delete" command can only target a single collection;
        meanwhile, it's possible for documents in two collections to reference one another.
        """

        # Initialize the response.
        response = MongoCommandResponse(ok=0)

        # Invoke the appropriate method based upon the kind of command.
        if isinstance(command, CountCommand):
            response = self._process_count_command(command)
        elif isinstance(command, CollStatsCommand):
            response = self._process_collstats_command(command)
        elif isinstance(command, DeleteCommand):
            response = self._process_delete_command(
                command, allow_broken_refs=allow_broken_refs
            )

            # If no documents were deleted, the user might have made a mistake. In that case,
            # we return an error response as a courtesy.
            if response.n == 0:
                raise HTTPException(
                    status_code=status.HTTP_418_IM_A_TEAPOT,
                    detail="No documents were deleted. Check the syntax of your request.",
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported command: {command}",
            )

        # Return the response.
        return response
