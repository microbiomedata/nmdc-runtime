import os
from datetime import datetime, timezone, timedelta
import hashlib
from importlib import import_module

from fastapi import HTTPException, status
from toolz import keyfilter

API_SITE_ID = os.getenv("API_SITE_ID")


def omit(blacklist, d):
    return keyfilter(lambda k: k not in blacklist, d)


def pick(whitelist, d):
    return keyfilter(lambda k: k in whitelist, d)


def md5hash_from(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def sha256hash_from(file_path: str):
    # https://stackoverflow.com/a/55542529
    h = hashlib.sha256()

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


def expiry_dt_from_now(days=0, hours=0, minutes=0, seconds=0):
    return datetime.now(timezone.utc) + timedelta(
        days=days, hours=hours, minutes=minutes, seconds=seconds
    )


def has_passed(dt):
    return datetime.now(timezone.utc) > dt


def import_via_dotted_path(dotted_path: str):
    module_name, _, member_name = dotted_path.rpartition(".")
    return getattr(import_module(module_name), member_name)


def dotted_path_for(member):
    return f"{member.__module__}.{member.__name__}"
