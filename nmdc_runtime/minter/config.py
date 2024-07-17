import os
from functools import lru_cache
from typing import List

from nmdc_runtime.util import get_nmdc_jsonschema_dict

from nmdc_runtime.api.db.mongo import get_mongo_db


@lru_cache()
def minting_service_id() -> str | None:
    return os.getenv("MINTING_SERVICE_ID")


def extract_typecode_from_pattern(pattern: str) -> str:
    r"""
    Returns the typecode portion of the specified string.

    >>> extract_typecode_from_pattern("foo-123-456$")  # original behavior
    'foo'
    >>> extract_typecode_from_pattern("(foo)-123-456$")  # returns first and only typecode
    'foo'
    >>> extract_typecode_from_pattern("(foo|bar)-123-456$")  # returns first of 2 typecodes
    'foo'
    >>> extract_typecode_from_pattern("(foo|bar|baz)-123-456$")  # returns first of > 2 typecodes
    'foo'
    """

    # Get the portion of the pattern preceding the first hyphen.
    # e.g. "foo-bar-baz" → ["foo", "bar-baz"] → "foo"
    typecode_sub_pattern = pattern.split("-", maxsplit=1)[0]

    # If that portion of the pattern is enclosed in parentheses, get the portion between the parentheses.
    # e.g. "(apple|banana|carrot)" → "apple|banana|carrot"
    if typecode_sub_pattern.startswith("(") and typecode_sub_pattern.endswith(")"):
        inner_pattern = typecode_sub_pattern[1:-1]

        # Finally, get everything before the first `|`, if any.
        # e.g. "apple|banana|carrot" → "apple"
        # e.g. "apple" → "apple"
        typecode = inner_pattern.split("|", maxsplit=1)[0]
    else:
        # Note: This is the original behavior, before we added support for multi-typecode patterns.
        # e.g. "apple" → "apple"
        typecode = typecode_sub_pattern

    return typecode


@lru_cache()
def typecodes() -> List[dict]:
    r"""
    Returns a list of dictionaries containing typecodes and associated information derived from the schema.

    Preconditions about the schema:
    - The typecode portion of the pattern is between the pattern prefix and the first subsequent hyphen.
    - The typecode portion of the pattern either consists of a single typecode verbatim (e.g. "foo");
      or consists of multiple typecodes in a pipe-delimited list enclosed in parentheses (e.g. "(foo|bar|baz)").
    - The typecode portion of the pattern does not, itself, contain any hyphens.

    TODO: Get the typecodes in a different way than by extracting them from a larger string, which seems brittle to me.
          Getting them a different way may require schema authors to _define_ them a different way (e.g. defining them
          in a dedicated property of a class; for example, one named `typecode`).
    """
    id_pattern_prefix = r"^(nmdc):"

    rv = []
    schema_dict = get_nmdc_jsonschema_dict()
    for cls_name, defn in schema_dict["$defs"].items():
        match defn.get("properties"):
            case {"id": {"pattern": p}} if p.startswith(id_pattern_prefix):
                # Get the portion of the pattern following the prefix.
                # e.g. "^(nmdc):foo-bar-baz" → "foo-bar-baz"
                index_of_first_character_following_prefix = len(id_pattern_prefix)
                pattern_without_prefix = p[index_of_first_character_following_prefix:]

                rv.append(
                    {
                        "id": "nmdc:" + cls_name + "_" + "typecode",
                        "schema_class": "nmdc:" + cls_name,
                        "name": extract_typecode_from_pattern(pattern_without_prefix),
                    }
                )
            case _:
                pass
    return rv


@lru_cache()
def shoulders():
    return [
        {
            "id": "nmdc:minter_shoulder_11",
            "assigned_to": "nmdc:minter_service_11",
            "name": "11",
        },
    ]


@lru_cache
def services():
    return [{"id": "nmdc:minter_service_11", "name": "central minting service"}]


def requesters():
    """not cached because sites are created dynamically"""
    return [{"id": s["id"]} for s in get_mongo_db().sites.find()]


@lru_cache()
def schema_classes():
    return [{"id": d["schema_class"]} for d in typecodes()]
