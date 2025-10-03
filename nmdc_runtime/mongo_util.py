import asyncio
import functools
from functools import lru_cache

from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase as AsyncMongoDatabase
from pymongo.database import Database as SyncMongoDatabase
from pymongo.collection import Collection as SyncMongoCollection
from typing import Optional, Any
from pymongo.errors import OperationFailure

from nmdc_runtime.config import environ


def get_mongo_client(
    host: str = environ.get("MONGO_HOST"),
    username: str = environ.get("MONGO_USERNAME"),
    password: str = environ.get("MONGO_PASSWORD"),
) -> AsyncMongoClient:
    r"""
    Returns an `AsyncMongoClient` instance you can use to access the MongoDB server specified via environment variables.
    Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_clientAsyncMongoClient
    """
    return AsyncMongoClient(
        host=host,
        username=username,
        password=password,
        directConnection=True,
        # Make the `retryWrites=True` default explicit.
        retryWrites=True,
    )


class RuntimeAsyncMongoDatabase:
    @classmethod
    def from_synchronous_database(cls, database: SyncMongoDatabase):
        self = cls()
        self._database = database
        self._client = database.client
        self._initialized = True
        return self

    def __init__(self):
        self._database: Optional[AsyncMongoDatabase | SyncMongoDatabase] = None
        self._client: Optional[AsyncMongoClient | MongoClient] = None
        self._initialized: bool = False

    def shallow_copy_for_dbname_iff_initialized(
        self, dbname: str
    ) -> "RuntimeAsyncMongoDatabase":
        """Returns a new `RuntimeAsyncMongoDatabase` object that shares the same underlying `AsyncMongoClient`."""
        if not self._initialized:
            raise Exception("Can shallow-copy if and only if `self` is initialized.")
        other = RuntimeAsyncMongoDatabase()
        other._database = self._client.get_database(dbname)
        other._client = self._client
        other._initialized = True
        return other

    @property
    def is_actually_async(self):
        return self._database is not None and isinstance(
            self._database, AsyncMongoDatabase
        )

    @property
    def raw(self) -> AsyncMongoDatabase | SyncMongoDatabase | None:
        return (
            self._database
            if self.is_actually_async
            else AwaitableSyncMongoDatabase(self._database)
        )

    async def initialize(
        self,
        host: str = environ.get("MONGO_HOST"),
        username: str = environ.get("MONGO_USERNAME"),
        password: str = environ.get("MONGO_PASSWORD"),
        dbname: str = environ.get("MONGO_DBNAME"),
    ):
        """Ensure singleton a shared dependency for injection into FastAPI path operations.

        Approach taken as best practice from:
        https://github.com/polyneme/fastapi-mongodb-stack-to-do-app/blob/31ca8a267d74e7bfc4e4850278c97a7b4912b8d7
         /backend/src/todo/server.py#L18
         (forked from <https://github.com/mongodb-developer/farm-stack-to-do-app>)
        """
        if self._initialized:
            return

        # Startup:
        client = get_mongo_client(host=host, username=username, password=password)
        database = client.get_database(dbname)
        # Ensure the database is available:
        pong = await database.command("ping")
        if int(pong["ok"]) != 1:
            raise Exception("Cluster connection is not okay!")
        # Check whether the application can write to the database:
        healthcheck_collection = database.get_collection("_runtime.healthcheck")
        try:
            await healthcheck_collection.insert_one({"status": "ok"})
            await healthcheck_collection.delete_many({"status": "ok"})
        except OperationFailure as e:
            raise Exception(f"Application cannot write to database. Error: {e}") from e
        self._database = database
        self._client = database.client
        self._initialized = True

    async def close(self):
        """Cleanup method for graceful shutdown"""
        if self._client is not None:
            await self._client.close()
        self._initialized = False

    async def rollback_session(self):
        """
        Yields a session with a started transaction that will be aborted when the session ends.

        Useful for testing, as any written data will be cleaned up.

        To end the session, call `session.end_session()`.
        """
        async with self._client.start_session() as session:
            await session.start_transaction()
            try:
                yield session
            finally:
                await session.abort_transaction()

    async def transaction_session(self):
        """Yields a session with a started transaction that will be automatically commit when the session ends.

        To end the session, call `session.end_session()`.
        """
        # Be explicit about the default `causal_consistency=True`.
        async with self._client.start_session(causal_consistency=True) as session:
            async with await session.start_transaction():
                yield session


_runtime_mdb_singleton: Optional[RuntimeAsyncMongoDatabase] = None


async def get_runtime_mdb() -> RuntimeAsyncMongoDatabase:
    """Return the (initialized) `RuntimeAsyncMongoDatabase` singleton, suitable for FastAPI dependency injection."""
    global _runtime_mdb_singleton

    if _runtime_mdb_singleton is None:
        _runtime_mdb_singleton = RuntimeAsyncMongoDatabase()
        await _runtime_mdb_singleton.initialize()
    return _runtime_mdb_singleton


@lru_cache
def get_synchronous_mongo_db():
    return MongoClient(
        host=environ.get("MONGO_HOST"),
        username=environ.get("MONGO_USERNAME"),
        password=environ.get("MONGO_PASSWORD"),
        directConnection=True,
        # Make the `retryWrites=True` default explicit.
        retryWrites=True,
    ).get_database(environ.get("MONGO_DBNAME"))


def _wrap_with_asyncio_executor(object_: Any, name: str):
    attr = getattr(object_, name)

    if callable(attr):

        @functools.wraps(attr)
        async def async_method(*args, **kwargs):
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None, functools.partial(attr, *args, **kwargs)
            )

        return async_method

    return attr


class AwaitableSyncMongoCollection:
    """
    A wrapper around `pymongo.collection.Collection` that makes methods awaitable.
    """

    def __init__(self, collection: SyncMongoCollection):
        self._collection = collection

    def __getattr__(self, name: str):
        return _wrap_with_asyncio_executor(self._collection, name)


class AwaitableSyncMongoDatabase:
    """A wrapper around `pymongo.database.Database` that makes methods awaitable."""

    def __init__(self, database: SyncMongoDatabase):
        self._database = database

    def __getattr__(self, name: str):
        return _wrap_with_asyncio_executor(self._database, name)

    def __getitem__(self, name: str) -> AwaitableSyncMongoCollection:
        """Get a collection by name using bracket notation."""
        return AwaitableSyncMongoCollection(self._database[name])

    def get_collection(self, name: str, **kwargs) -> AwaitableSyncMongoCollection:
        """Get a collection with the given name and options."""
        return AwaitableSyncMongoCollection(
            self._database.get_collection(name, **kwargs)
        )

    @property
    def client(self):
        """Access to the underlying client."""
        return self._database.client

    @property
    def name(self):
        """Database name."""
        return self._database.name
