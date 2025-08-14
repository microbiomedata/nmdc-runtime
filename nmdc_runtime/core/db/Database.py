r"""
TODO: Delete this module if it is obsolete.
"""

from contextlib import contextmanager

from motor import motor_asyncio


class Database:
    def __init__(self, db_url: str) -> None:
        self._client = motor_asyncio.AsyncIOMotorClient(db_url)
        self._db = self._client["database"]

    @contextmanager
    def session(self):
        return self._db
