from json import dumps
from logging import getLogger
from typing import Any

from bson.json_util import loads
from pymongo.database import Database

from nmdc_runtime.api.models.query import (
    Cmd as MongoCommand,
    CommandResponse as MongoCommandResponse,
    CollStatsCommand,
    CollStatsCommandResponse,
    CountCommand,
    CountCommandResponse,
)


logger = getLogger(__name__)

class MongoCommandProcessor:
    """
    Note: This class was created as a replacement for the `_run_mdb_cmd` function defined in the
          `nmdc_runtime/api/endpoints/queries.py` file. That function had grown over time to be
          over 400 lines, to have multiple responsibilities, and to accumulate caveats/TODOs.
          This class, initially, shares some of those traits, but we think it'll be easier for
          us to work with over time.
    """

    def __init__(self, db: Database):
        """Initialize this instance's Mongo database reference to be the specified one."""
        self.db = db

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

        # Return the response.
        return response
