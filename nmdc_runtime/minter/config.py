import os
from functools import lru_cache

from nmdc_runtime.util import get_nmdc_jsonschema_dict

from nmdc_runtime.api.db.mongo import get_mongo_db


@lru_cache()
def minting_service_id() -> str | None:
    return os.getenv("MINTING_SERVICE_ID")


@lru_cache()
def typecodes():
    rv = []
    schema_dict = get_nmdc_jsonschema_dict()
    for cls_name, defn in schema_dict["$defs"].items():
        match defn.get("properties"):
            case {"id": {"pattern": p}} if p.startswith("^(nmdc):"):
                rv.append(
                    {
                        "id": "nmdc:" + cls_name + "_" + "typecode",
                        "schema_class": "nmdc:" + cls_name,
                        "name": p.split(":", maxsplit=1)[-1].split("-", maxsplit=1)[0],
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
