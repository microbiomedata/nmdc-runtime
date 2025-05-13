import os
import warnings

if os.getenv("API_HOST"):
    warnings.warn(
        "The `API_HOST` environment variable is deprecated; use `API_BASE_URL` instead.",
        DeprecationWarning,
    )
BASE_URL_INTERNAL = os.getenv("API_BASE_URL", os.getenv("API_HOST"))


if os.getenv("API_HOST_EXTERNAL"):
    warnings.warn(
        "The `API_HOST_EXTERNAL` environment variable is deprecated; "
        "use `API_BASE_URL_EXTERNAL` or `API_URL_{SCHEME,HOST,PORT}_EXTERNAL` instead.",
        DeprecationWarning,
    )
API_BASE_URL_EXTERNAL = (
    os.getenv("API_HOST_EXTERNAL")
    or os.getenv("API_BASE_URL_EXTERNAL")
    or (
        os.getenv("API_URL_SCHEME_EXTERNAL", "http")
        + "://"
        + os.getenv("API_URL_HOST_EXTERNAL", "127.0.0.1")
        + ":"
        + os.getenv("API_URL_PORT_EXTERNAL", os.getenv("MY_DYNAMIC_HOST_PORT"))
    )
)


DRS_API_BASE_URL_EXTERNAL = "drs://" + API_BASE_URL_EXTERNAL.split("://", 1)[-1]

DATABASE_CLASS_NAME = "Database"
