import json
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path

import fastjsonschema
import requests
from frozendict import frozendict
from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict
from toolz import merge

from nmdc_runtime.api.core.util import sha256hash_from_file
from nmdc_runtime.api.models.object import DrsObjectIn

nmdc_jsonschema = get_nmdc_jsonschema_dict()
nmdc_jsonschema_validate = fastjsonschema.compile(nmdc_jsonschema)

REPO_ROOT_DIR = Path(__file__).parent.parent


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


def pluralize(singular, using, pluralized=None):
    """Pluralize a word for output.

    >>> pluralize("job", 1)
    'job'
    >>> pluralize("job", 2)
    'jobs'
    >>> pluralize("datum", 2, "data")
    'data'
    """
    return (
        singular
        if using == 1
        else (pluralized if pluralized is not None else f"{singular}s")
    )


def iterable_from_dict_keys(d, keys):
    for k in keys:
        yield d[k]
