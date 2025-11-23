"""
This module acts as a unified interface between the codebase and the environment.
We will eventually move all of the Runtime's environment variables reads into this
module, instead of leaving them sprinkled throughout the codebase.

TODO: Move all environment variable reads into this module and update references accordingly.
"""

from typing import Set
import os


def is_env_var_true(name: str, default: str = "false") -> bool:
    r"""
    Checks whether the value of the specified environment variable
    meets our criteria for true-ness.

    Reference: https://docs.python.org/3/library/os.html#os.environ

    Run doctests via: $ python -m doctest nmdc_runtime/config.py

    >>> import os
    >>> name = "EXAMPLE_ENV_VAR"
    >>> os.unsetenv(name)  # Undefined
    >>> is_env_var_true(name)
    False
    >>> is_env_var_true(name, "true")  # Undefined, overridden default
    True
    >>> os.environ[name] = "false"  # Defined as false
    >>> is_env_var_true(name)
    False
    >>> os.environ[name] = "true"  # Defined as true
    >>> is_env_var_true(name)
    True
    >>> os.environ[name] = "TRUE"  # Case-insensitive
    >>> is_env_var_true(name)
    True
    >>> os.environ[name] = "potato"  # Non-boolean string
    >>> is_env_var_true(name)
    False
    """
    lowercase_true_strings: Set[str] = {"true"}
    return os.environ.get(name, default).lower() in lowercase_true_strings


# Feature flag to enable/disable the `/nmdcschema/linked_instances` endpoint and the tests that target it.
IS_LINKED_INSTANCES_ENDPOINT_ENABLED: bool = is_env_var_true(
    "IS_LINKED_INSTANCES_ENDPOINT_ENABLED", default="true"
)

# Feature flag that can be used to enable/disable the `/scalar` endpoint.
IS_SCALAR_ENABLED: bool = is_env_var_true("IS_SCALAR_ENABLED", default="true")

# Feature flag that can be used to enable/disable performance profiling,
# which can be activated via the `?profile=true` URL query parameter.
IS_PROFILING_ENABLED: bool = is_env_var_true("IS_PROFILING_ENABLED", default="false")

# ────────────────────────────────────────────────────────────────────────────┐
# Sentry
# ────────────────────────────────────────────────────────────────────────────┘

# Feature flag to enable (true) or disable (false) the Sentry integration. When this
# is false, none of the other `SENTRY_*` environment variables come into play.
IS_SENTRY_ENABLED: bool = is_env_var_true("IS_SENTRY_ENABLED", default="false")

# The Sentry DSN (Data Source Name) you want the Sentry SDK to use. This URL is specific
# to a Sentry project and can be obtained from the Sentry project's dashboard.
# Docs: https://docs.sentry.io/concepts/key-terms/dsn-explainer/
SENTRY_DSN: str = os.environ.get("SENTRY_DSN", "")

# The name of the environment (e.g., "production", "development", "local", "unknown")
# on Sentry within which you want data sent from this application instance to be stored.
# Docs: https://docs.sentry.io/platforms/python/configuration/environments/
SENTRY_ENVIRONMENT: str = os.environ.get("SENTRY_ENVIRONMENT", "unknown")

# The percentage of all transactions (0.0 is 0%, 1.0 is 100%) that you want Sentry
# to _capture_ (i.e., you want to be sent to Sentry).
# Docs: https://docs.sentry.io/platforms/python/tracing/#configure
SENTRY_TRACES_SAMPLE_RATE: float = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.0"))

# The percentage of sampled transactions (0.0 is 0%, 1.0 is 100%) that you want Sentry
# to _profile_ (i.e., about which you want to send even more details to Sentry).
# Docs: https://docs.sentry.io/platforms/python/profiling/#managing-profile-sampling-rates
SENTRY_PROFILES_SAMPLE_RATE: float = float(os.environ.get("SENTRY_PROFILES_SAMPLE_RATE", "0.0"))
