import os
from functools import lru_cache

from terminusdb_client import WOQLClient

_state = {"client": None}


@lru_cache
def get_terminus_db():
    if _state["client"] is None:
        server_url = os.getenv("TERMINUS_SERVER_URL")
        key = os.getenv("TERMINUS_KEY")
        user = os.getenv("TERMINUS_USER")
        account = os.getenv("TERMINUS_ACCOUNT")
        dbid = os.getenv("TERMINUS_DBID")
        _client = WOQLClient(server_url=server_url)
        _client.connect(user=user, key=key, account=account)
        db_info = _client.get_database(dbid=dbid)
        if db_info is None:
            _client.create_database(dbid=dbid, label=dbid)
        _client.connect(user=user, key=key, account=account, db=dbid)
        _state["client"] = _client
    return _state["client"]
