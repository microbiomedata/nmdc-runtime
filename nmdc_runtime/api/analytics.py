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

_requests = []
_last_posted = datetime.now()


def _post_requests(collection: str, requests_data: List[Dict], source: str):
    mdb = get_mongo_db()
    mdb[collection].insert_many([merge(d, {"source": source}) for d in requests_data])


def log_request(collection: str, request_data: Dict, source: str = "FastAPI"):
    global _requests, _last_posted
    _requests.append(request_data)
    now = datetime.now()
    # flush queue every minute at most
    if (now - _last_posted).total_seconds() > 60.0:
        threading.Thread(
            target=_post_requests, args=(collection, _requests, source)
        ).start()
        _requests = []
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

        # Build a dictionary that describes the incoming request.
        #
        # Note: `request.headers` is an instance of `MultiDict`. References:
        #       - https://www.starlette.io/requests/#headers
        #       - https://multidict.aio-libs.org/en/stable/multidict/
        #
        request_data = {
            "hostname": request.url.hostname,
            "ip_address": request.client.host,
            "path": request.url.path,
            "user_agent": request.headers.get("user-agent"),
            "method": request.method,
            "status": response.status_code,
            "response_time": int((time() - start) * 1000),
            "created_at": datetime.now().isoformat(),
        }

        log_request(self.collection, request_data, "FastAPI")
        return response
