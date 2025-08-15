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
        is_profiling = isinstance(profile_param, str) and profile_param.lower() == "true"

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
