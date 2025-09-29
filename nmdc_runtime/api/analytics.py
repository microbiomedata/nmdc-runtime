"""
Based on <https://github.com/tom-draper/api-analytics/tree/main/analytics/python/fastapi>
under MIT License <https://github.com/tom-draper/api-analytics/blob/main/analytics/python/fastapi/LICENSE>
"""

from datetime import datetime
from time import time
from typing import Dict, List

from pymongo.errors import OperationFailure, BulkWriteError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from toolz import merge

from nmdc_runtime.mongo_util import get_runtime_mdb

# This is a queue of the "request descriptors" that we will eventually insert into the database.
_requests = []
_last_post_attempt = datetime.now()


async def _post_requests(collection: str, requests_data: List[Dict], source: str):
    """Inserts the specified request descriptors into the specified MongoDB collection."""
    mdb = await get_runtime_mdb()
    await mdb.raw[collection].insert_many(
        [merge(d, {"source": source}) for d in requests_data]
    )


async def log_request(collection: str, request_data: Dict, source: str = "FastAPI"):
    """Flushes the queue of request descriptors to the database if enough time has passed since the previous time."""
    global _requests, _last_post_attempt
    _requests.append(request_data)
    now = datetime.now()
    # flush queue every minute at most
    if (now - _last_post_attempt).total_seconds() > 60.0:
        # Note: There is no race condition here because the FastAPI application has a single event loop.
        # Note: The approach here was based on one linked to at the top of this module.
        try:
            await _post_requests(collection, _requests, source)
            _requests = []  # empties the queue
            _last_post_attempt = now
        except (BulkWriteError, OperationFailure):
            # If insertion fails, retain `_requests` in the queue and retry on the next attempt.
            _last_post_attempt = now


class Analytics(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, collection: str = "_runtime.analytics"):
        super().__init__(app)
        self.collection = collection

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start = time()
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
            "created_at": datetime.now().isoformat(),
        }

        await log_request(self.collection, request_data, "FastAPI")
        return response
