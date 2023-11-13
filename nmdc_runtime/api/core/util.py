import hashlib
import json
import os
import secrets
import string
from datetime import datetime, timezone, timedelta
from importlib import import_module

from fastapi import HTTPException, status
from pydantic import BaseModel
from toolz import keyfilter

API_SITE_ID = os.getenv("API_SITE_ID")
API_SITE_CLIENT_ID = os.getenv("API_SITE_CLIENT_ID")


def omit(blacklist, d):
    return keyfilter(lambda k: k not in blacklist, d)


def pick(whitelist, d):
    return keyfilter(lambda k: k in whitelist, d)


def hash_from_str(s: str, algo="sha256") -> str:
    if algo not in hashlib.algorithms_guaranteed:
        raise ValueError(f"desired algorithm {algo} not supported")
    return getattr(hashlib, algo)(s.encode("utf-8")).hexdigest()


def sha256hash_from_file(file_path: str, timestamp: str):
    # https://stackoverflow.com/a/55542529
    h = hashlib.sha256()

    timestamp_bytes = timestamp.encode("utf-8")
    h.update(timestamp_bytes)

    with open(file_path, "rb") as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def raise404_if_none(doc, detail="Not found"):
    if doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    return doc


def now(as_str=False):
    dt = datetime.now(timezone.utc)
    return dt.isoformat() if as_str else dt


def expiry_dt_from_now(days=0, hours=0, minutes=0, seconds=0):
    return now() + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def has_passed(dt):
    return now() > dt


def import_via_dotted_path(dotted_path: str):
    module_name, _, member_name = dotted_path.rpartition(".")
    return getattr(import_module(module_name), member_name)


def dotted_path_for(member):
    return f"{member.__module__}.{member.__name__}"


def generate_secret(length=12):
    """Generate a secret.

    With
    - at least one lowercase character,
    - at least one uppercase character, and
    - at least three digits

    """
    if length < 8:
        raise ValueError(f"{length=} must be >=8.")
    alphabet = string.ascii_letters + string.digits + "!@#$%^*-_+="
    # based on https://docs.python.org/3.8/library/secrets.html#recipes-and-best-practices
    while True:
        _secret = "".join(secrets.choice(alphabet) for i in range(length))
        if (
            any(c.islower() for c in _secret)
            and any(c.isupper() for c in _secret)
            and sum(c.isdigit() for c in _secret) >= 3
        ):
            break
    return _secret


def json_clean(data, model, exclude_unset=False) -> dict:
    """Run data through a JSON serializer for a pydantic model."""
    if not isinstance(data, (dict, BaseModel)):
        raise TypeError("`data` must be a pydantic model or its .model_dump()")
    m = model(**data) if isinstance(data, dict) else data
    return json.loads(m.json(exclude_unset=exclude_unset))
