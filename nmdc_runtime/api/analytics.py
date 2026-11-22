"""
Based on <https://github.com/tom-draper/api-analytics/tree/main/analytics/python/fastapi>
under MIT License <https://github.com/tom-draper/api-analytics/blob/main/analytics/python/fastapi/LICENSE>
"""

from collections import defaultdict
from datetime import datetime
import threading
from time import time
from typing import Dict, List, Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from toolz import merge

from nmdc_runtime.api.db.mongo import get_mongo_db

# This is a queue of the "request descriptors" that we will eventually insert into the database.
_requests = []
_last_posted = datetime.now()


def get_analytics_collection_name(collection_prefix: str, timestamp: datetime) -> str:
    """Return the year-month-partitioned collection name for `timestamp`.

    >>> from datetime import datetime
    >>> get_analytics_collection_name("_runtime.analytics", datetime(2026, 11, 21, 14, 3, 25))
    '_runtime.analytics.2026_11'
    >>> get_analytics_collection_name("_runtime.analytics", datetime(2025, 3, 5, 0, 0, 0))
    '_runtime.analytics.2025_03'
    >>> get_analytics_collection_name("custom_prefix", datetime(2025, 3, 5))
    'custom_prefix.2025_03'
    """
    return f"{collection_prefix}.{timestamp.strftime('%Y_%m')}"


def get_request_timestamp(
    request_data: Dict, fallback: Optional[datetime] = None
) -> datetime:
    """Extract `created_at` from `request_data` as a `datetime`, or return `fallback`.

    >>> from datetime import datetime
    >>> fb = datetime(2026, 11, 1, 0, 0, 0)
    >>> get_request_timestamp({"created_at": "2026-10-31T23:59:59.123456"}, fallback=fb)
    datetime.datetime(2026, 10, 31, 23, 59, 59, 123456)
    >>> get_request_timestamp({"path": "/health"}, fallback=fb)
    datetime.datetime(2026, 11, 1, 0, 0)
    >>> get_request_timestamp({"created_at": "not-a-timestamp"}, fallback=fb)
    datetime.datetime(2026, 11, 1, 0, 0)
    """
    created_at = request_data.get("created_at")
    if isinstance(created_at, str):
        try:
            return datetime.fromisoformat(created_at)
        except ValueError:
            pass
    return fallback if fallback is not None else datetime.now()


def group_requests_by_collection(
    collection_prefix: str,
    requests_data: List[Dict],
    source: str,
    fallback_timestamp: Optional[datetime] = None,
) -> Dict[str, List[Dict]]:
    """Group request descriptors by target year-month collection and attach `source`.

    >>> from datetime import datetime
    >>> fb = datetime(2026, 11, 15, 12, 0, 0)
    >>> group_requests_by_collection("_runtime.analytics", [], "FastAPI", fb)
    {}
    >>> requests_data = [
    ...     {"path": "/a", "created_at": "2026-10-31T23:59:59"},
    ...     {"path": "/b", "created_at": "2026-11-01T00:00:01"},
    ...     {"path": "/b", "created_at": "2026-11-02T00:00:01"},
    ...     {"path": "/c"},
    ... ]
    >>> requests_by_collection_name = group_requests_by_collection(
    ...     collection_prefix="_runtime.analytics",
    ...     requests_data=requests_data,
    ...     source="FastAPI",
    ...     fallback_timestamp=fb,
    ... )
    >>> sorted(requests_by_collection_name.keys())
    ['_runtime.analytics.2026_10', '_runtime.analytics.2026_11']
    >>> len(requests_by_collection_name["_runtime.analytics.2026_10"])
    1
    >>> len(requests_by_collection_name["_runtime.analytics.2026_11"])
    3
    >>> requests_by_collection_name["_runtime.analytics.2026_10"]
    [{'path': '/a', 'created_at': '2026-10-31T23:59:59', 'source': 'FastAPI'}]
    >>> [d["path"] for d in requests_by_collection_name["_runtime.analytics.2026_11"]]
    ['/b', '/b', '/c']
    """
    if len(requests_data) == 0:
        return {}

    documents_by_collection_name: Dict[str, List[Dict]] = defaultdict(list)
    for request_data in requests_data:
        request_timestamp = get_request_timestamp(request_data, fallback=fallback_timestamp)
        collection_name = get_analytics_collection_name(collection_prefix, request_timestamp)
        document = merge(request_data, {"source": source})
        documents_by_collection_name[collection_name].append(document)

    return dict(documents_by_collection_name)  # converts `defaultdict` into regular `dict`


def _post_requests(collection_name: str, requests_data: List[Dict], source: str):
    """
    Inserts request descriptors into year/month-specific MongoDB collections based on each request's
    `created_at` timestamp.
    """
    mdb = get_mongo_db()
    request_descriptors_by_collection_name = group_requests_by_collection(
        collection_prefix=collection_name,
        requests_data=requests_data,
        source=source,
    )
    for target_collection_name, documents in request_descriptors_by_collection_name.items():
        mdb[target_collection_name].insert_many(documents)


def log_request(collection: str, request_data: Dict, source: str = "FastAPI"):
    """Flushes the queue of request descriptors to the database if enough time has passed since the previous time."""
    global _requests, _last_posted
    _requests.append(request_data)
    now = datetime.now()
    # flush queue every minute at most
    if (now - _last_posted).total_seconds() > 60.0:
        # Note: This use of threading is an attempt to avoid blocking the current thread
        #       while performing the insertion(s).
        #
        # TODO: Is there is a race condition here? If multiple requests arrive at approximately
        #       the same time, is it possible that each one causes a different thread to be
        #       started, each with a different (and possibly overlapping) set of requests to
        #       insert?
        #
        # TODO: If the insertion fails, will the requests be lost?
        #
        # Note: The author of this function said it may have been a "standard" solution copied
        #       from some documentation. Indeed, the comment at the top of this module contains
        #       a link to code on which it was based.
        #
        threading.Thread(
            target=_post_requests, args=(collection, _requests, source)
        ).start()
        _requests = []  # empties the queue
        _last_posted = now


class Analytics(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, collection: str = "_runtime.analytics"):
        super().__init__(app)
        self.collection = collection

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start = time()
        request_received_at = datetime.now()
        response = await call_next(request)

        # Use a fallback IP address value (currently an empty string) if we can't derive one from the request.
        ip_address: str = "" if request.client is None else request.client.host

        # Build a dictionary that describes the incoming request.
        #
        # Note: `request.headers` is an instance of `MultiDict`. References:
        #       - https://www.starlette.io/requests/#headers
        #       - https://multidict.aio-libs.org/en/stable/multidict/
        #
        request_data = {
            "hostname": request.url.hostname,
            "ip_address": ip_address,
            "path": request.url.path,
            "user_agent": request.headers.get("user-agent"),
            "method": request.method,
            "status": response.status_code,
            "response_time": int((time() - start) * 1000),
            "created_at": request_received_at.isoformat(),
        }

        log_request(self.collection, request_data, "FastAPI")
        return response
