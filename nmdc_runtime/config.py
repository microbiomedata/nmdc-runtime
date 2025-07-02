"""
This module acts as a unified interface between the codebase and the environment.
We will eventually move all of the Runtime's environment variables reads into this
module, instead of leaving them sprinkled throughout the codebase.

TODO: Move all environment variable reads into this module and update references accordingly.
"""

from typing import Set
import os


# Define some helper variables.
lowercase_true_strings: Set[str] = {"true"}

# The name of the schema class representing the database. We don't bother to
# make this customizable via the environment, as we expect it to never change.
DATABASE_CLASS_NAME: str = "Database"

# Feature flag that can be used to enable/disable the `/nmdcschema/related_ids`
# endpoint and the tests that target it. The value is read from the environment.
IS_RELATED_IDS_ENDPOINT_ENABLED: bool = os.environ.get("IS_RELATED_IDS_ENDPOINT_ENABLED", "true").lower() in lowercase_true_strings
