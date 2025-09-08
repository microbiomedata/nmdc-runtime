"""
Based on <https://github.com/tom-draper/api-analytics/tree/main/analytics/python/fastapi>
under MIT License <https://github.com/tom-draper/api-analytics/blob/main/analytics/python/fastapi/LICENSE>
"""

from datetime import datetime
import threading
from time import time
from typing import Dict, List

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from toolz import merge

from nmdc_runtime.api.db.mongo import get_mongo_db

# This is a queue of the "request descriptors" that we will eventually insert into the database.
_requests = []
_last_posted = datetime.now()


def _post_requests(collection: str, requests_data: List[Dict], source: str):
    """Inserts the specified request descriptors into the specified MongoDB collection."""
    mdb = get_mongo_db()
    mdb[collection].insert_many([merge(d, {"source": source}) for d in requests_data])


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
        response = await call_next(request)

        # Apply a fallback IP address if we can't determine one from the request.
        ip_address: str = request.client.host if request.client is not None else ""

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

        log_request(self.collection, request_data, "FastAPI")
        return response
