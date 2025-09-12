import logging
import time

from fastapi import Request
from fastapi.responses import HTMLResponse
from pyinstrument import Profiler
from starlette.middleware.base import BaseHTTPMiddleware


class PyinstrumentMiddleware(BaseHTTPMiddleware):
    r"""
    FastAPI middleware that uses Pyinstrument to do performance profiling.

    If the requested URL includes the query parameter, `profile=true`, this middleware
    will profile the performance of the application for the duration of the HTTP request,
    and then override the HTTP response to consist of a performance report.

    References:
    - https://pyinstrument.readthedocs.io/en/latest/guide.html#profile-a-web-request-in-fastapi
    - https://stackoverflow.com/a/71526036
    """

    async def dispatch(self, request: Request, call_next):
        # Get the `profile` query parameter and check whether its value is "true" (case insensitive).
        profile_param = request.query_params.get("profile", None)
        is_profiling = (
            isinstance(profile_param, str) and profile_param.lower() == "true"
        )

        # If profiling is enabled for this request, profile the request processing.
        if is_profiling:
            # Start the profiler.
            profiler = Profiler()
            profiler.start()

            # Allow the request to be processed as usual, and discard the normal response.
            _ = await call_next(request)

            # Stop the profiler.
            profiler.stop()

            # Override the normal response with the profiling report.
            return HTMLResponse(profiler.output_html())
        else:
            # Allow the request to be processed as usual.
            return await call_next(request)


class ResponseTimeLoggerMiddleware(BaseHTTPMiddleware):
    r"""
    FastAPI middleware that logs the response time to the console.

    This function measures how long the application takes to process the current HTTP request,
    and logs that duration to the console. It also logs the HTTP method and URL path of the request,
    although it represents the query string (if any) as "?..." instead of representing it in full.

    Reference: https://fastapi.tiangolo.com/tutorial/middleware/#before-and-after-the-response
    """

    async def dispatch(self, request: Request, call_next):
        start_time: float = time.perf_counter()
        response = await call_next(request)
        end_time: float = time.perf_counter()
        duration_in_seconds = end_time - start_time
        formatted_duration = f"{duration_in_seconds:.3f}"

        # Log the response time to the console.
        #
        # Note: We condense the query string (if any) to "?..." because we want the
        #       focal point of these messages to be the duration. Uvicorn's access
        #       logs already show the full query string.
        #
        query_string = "" if request.url.query in {None, ""} else "?..."
        request_string = f"{request.method} {request.url.path}{query_string}"
        logging.info(f'"{request_string}" response time (sec): {formatted_duration}')

        return response
