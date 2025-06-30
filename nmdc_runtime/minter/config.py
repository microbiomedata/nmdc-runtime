import os
from functools import lru_cache
from typing import List

from nmdc_schema.id_helpers import get_typecode_for_future_ids

from nmdc_runtime.util import get_nmdc_jsonschema_dict
from nmdc_runtime.api.db.mongo import get_mongo_db


@lru_cache()
def minting_service_id() -> str | None:
    return os.getenv("MINTING_SERVICE_ID")


@lru_cache()
def typecodes() -> List[dict]:
    r"""
    Returns a list of dictionaries containing typecodes and associated information derived from the schema.

    Note: In this function, we rely on a helper function provided by the `nmdc-schema` package to extract—from a given
          class's `id` slot's pattern—the typecode that the minter would use when generating an ID for an instance of
          that class _today_; regardless of what it may have used in the past.

    >>> typecode_descriptors = typecodes()
    # Test #1: We get the typecode we expect, for a class whose pattern contains only one typecode.
    >>> any((td["name"] == "sty" and td["schema_class"] == "nmdc:Study") for td in typecode_descriptors)
    True
    # Tests #2 and #3: We get only the typecode we expect, for a class whose pattern contains multiple typecodes.
    >>> any((td["name"] == "dgms" and td["schema_class"] == "nmdc:MassSpectrometry") for td in typecode_descriptors)
    True
    >>> any((td["name"] == "omprc" and td["schema_class"] == "nmdc:MassSpectrometry") for td in typecode_descriptors)
    False
    """
    id_pattern_prefix = r"^(nmdc):"

    rv = []
    schema_dict = get_nmdc_jsonschema_dict()
    for cls_name, defn in schema_dict["$defs"].items():
        match defn.get("properties"):
            case {"id": {"pattern": p}} if p.startswith(id_pattern_prefix):
                # Extract the typecode from the pattern.
                typecode_for_future_ids = get_typecode_for_future_ids(slot_pattern=p)

                rv.append(
                    {
                        "id": "nmdc:" + cls_name + "_" + "typecode",
                        "schema_class": "nmdc:" + cls_name,
                        "name": typecode_for_future_ids,
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
