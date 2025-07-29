from pymongo.database import Database
from pymongo.collection import Collection
from typing import Any, Optional
from pymongo.client_session import ClientSession
import inspect


def _wrap_with_session(obj: Any, name: str, session: Optional[ClientSession]) -> Any:
    """
    Wraps a callable attribute of an object to automatically include a session
    if the callable accepts a 'session' keyword argument.
    """
    attr = getattr(obj, name)
    if callable(attr):
        signature = inspect.signature(attr)
        parameters = signature.parameters
        accepts_session = any(
            param.name == "session"
            for param in parameters.values()
            if param.kind
            in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        )

        def wrapper(*args, **kwargs):
            if session is not None and accepts_session and "session" not in kwargs:
                kwargs["session"] = session
            return attr(*args, **kwargs)

        return wrapper
    return attr


class SessionBoundCollection:
    """
    A wrapper around pymongo.collection.Collection that automatically passes a session
    to methods that accept it.
    """

    def __init__(self, collection: Collection, session: Optional[ClientSession] = None):
        self._collection = collection
        self._session = session

    def __getattr__(self, name: str):
        return _wrap_with_session(self._collection, name, self._session)

    def __getitem__(self, name: str) -> "SessionBoundCollection":
        return SessionBoundCollection(self._collection[name], self._session)


class SessionBoundDatabase(Database):
    """
    A wrapper around pymongo.database.Database that automatically passes a session
    to methods that accept it.
    """

    def __init__(self, database: Database, session: Optional[ClientSession] = None):
        super().__init__(
            database.client,
            database.name,
            database.codec_options,
            database.read_preference,
            database.write_concern,
            database.read_concern,
        )
        self._database = database
        self._session = session

    def __getattr__(self, name: str):
        return _wrap_with_session(self._database, name, self._session)

    def __getitem__(self, name: str) -> SessionBoundCollection:
        return SessionBoundCollection(self._database[name], self._session)

    def get_collection(self, name: str, **kwargs) -> SessionBoundCollection:
        """Get a :class:`~pymongo.collection.Collection` with the given name and options."""
        collection = super().get_collection(name, **kwargs)
        return SessionBoundCollection(collection, self._session)

    @property
    def client(self):
        return self._database.client

    @property
    def unbounded(self):
        return self._database

    @property
    def name(self):
        return self._database.name
