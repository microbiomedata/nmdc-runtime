import os
from functools import lru_cache
from typing import List

from nmdc_runtime.util import get_nmdc_jsonschema_dict

from nmdc_runtime.api.db.mongo import get_mongo_db


@lru_cache()
def minting_service_id() -> str | None:
    return os.getenv("MINTING_SERVICE_ID")


@lru_cache()
def typecodes() -> List[dict]:
    r"""
    Returns a list of typecodes—plus related info—obtained from the schema.

    Preconditions:
    - The typecode portion of the pattern is between the pattern prefix and the first subsequent hyphen.
    - The typecode portion of the pattern either consists of a single typecode verbatim (e.g. "foo");
      or consists of multiple typecodes in a pipe-delimited list enclosed in parentheses (e.g. "(foo|bar|baz)").
    - The typecode portion of the pattern does not, itself, contain any hyphens.

    TODO: Get typecodes in a different way than by extracting them from a larger string, which seems brittle to me.
          Getting them a different way may require schema authors to _define_ them a different way (e.g. defining them
          in a dedicated property of a class, named `typecode`).
    """
    id_pattern_prefix = r"^(nmdc):"

    rv = []
    schema_dict = get_nmdc_jsonschema_dict()
    for cls_name, defn in schema_dict["$defs"].items():
        match defn.get("properties"):
            case {"id": {"pattern": p}} if p.startswith(id_pattern_prefix):
                # Get the portion of the pattern following the prefix.
                # e.g. "^(nmdc):foo-bar-baz" → "foo-bar-baz"
                pattern_without_prefix: str = p[len(id_pattern_prefix):]

                # Get the portion of the remaining pattern preceding the first hyphen.
                # e.g. "foo-bar-baz" → ["foo", "bar-baz"] → "foo"
                typecode_sub_pattern = pattern_without_prefix.split("-", maxsplit=1)[0]

                # If the remaining pattern is enclosed in parentheses, get the portion between them.
                # e.g. "(apple|banana|carrot)" → "apple|banana|carrot"
                if typecode_sub_pattern.startswith("(") and typecode_sub_pattern.endswith(")"):
                    inner_pattern = typecode_sub_pattern[1:-1]

                    # Finally, get everything before the first `|`, if any.
                    typecode = inner_pattern.split("|", maxsplit=1)[0]
                else:
                    # Note: This is the original behavior, before we added support for multi-typecode patterns.
                    typecode = typecode_sub_pattern

                rv.append(
                    {
                        "id": "nmdc:" + cls_name + "_" + "typecode",
                        "schema_class": "nmdc:" + cls_name,
                        "name": typecode,
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
