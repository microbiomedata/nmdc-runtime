import json
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path

import fastjsonschema
import nmdc_schema
import requests
from frozendict import frozendict
from toolz import merge

from nmdc_runtime.api.core.util import sha256hash_from_file
from nmdc_runtime.api.models.object import DrsObjectIn

# XXX change to use pkg_resources once pip package builds and deploys properly.
# NMDC_JSON_SCHEMA_PATH = pkg_resources.resource_filename("nmdc_schema", "nmdc.schema.json")
NMDC_JSON_SCHEMA_PATH = Path(nmdc_schema.__path__[0]).joinpath("nmdc.schema.json")


with open(NMDC_JSON_SCHEMA_PATH) as f:
    nmdc_jsonschema = json.load(f)
    nmdc_jsonschema_validate = fastjsonschema.compile(nmdc_jsonschema)


def put_object(filepath, url, mime_type=None):
    if mime_type is None:
        mime_type = mimetypes.guess_type(filepath)[0]
    with open(filepath, "rb") as f:
        return requests.put(url, data=f, headers={"Content-Type": mime_type})


def drs_metadata_for(filepath, base=None):
    """given file path, get drs metadata

    required: size, created_time, and at least one checksum.
    """
    base = {} if base is None else base
    if "size" not in base:
        base["size"] = os.path.getsize(filepath)
    if "created_time" not in base:
        base["created_time"] = datetime.fromtimestamp(
            os.path.getctime(filepath), tz=timezone.utc
        )
    if "checksums" not in base:
        base["checksums"] = [
            {"type": "sha256", "checksum": sha256hash_from_file(filepath)}
        ]
    if "mime_type" not in base:
        base["mime_type"] = mimetypes.guess_type(filepath)[0]
    if "name" not in base:
        base["name"] = Path(filepath).name
    return base


def drs_object_in_for(filepath, op_doc, base=None):
    access_id = f'{op_doc["metadata"]["site_id"]}:{op_doc["metadata"]["object_id"]}'
    drs_obj_in = DrsObjectIn(
        **drs_metadata_for(
            filepath,
            merge(base or {}, {"access_methods": [{"access_id": access_id}]}),
        )
    )
    return json.loads(drs_obj_in.json(exclude_unset=True))


def frozendict_recursive(obj):
    """Recursive function which turns dictionaries into
    FrozenDict objects, lists into tuples, and sets
    into frozensets.
    Can also be used to turn JSON data into a hasahable value.
    """

    try:
        # See if the object is hashable
        hash(obj)
        return obj
    except TypeError:
        pass

    if isinstance(obj, dict):
        return frozendict({k: frozendict_recursive(obj[k]) for k in obj})

    msg = "Unsupported type: %r" % type(obj).__name__
    raise TypeError(msg)
