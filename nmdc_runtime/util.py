import json
import mimetypes
import os
import pkgutil
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO
from itertools import chain
from pathlib import Path
from typing import Callable, List, Optional, Set, Dict, Tuple

import fastjsonschema
import requests
from bson.son import SON
from frozendict import frozendict
from linkml_runtime import SchemaView
from nmdc_schema.get_nmdc_view import ViewGetter
from pymongo.database import Database as MongoDatabase
from pymongo.errors import OperationFailure
from refscan.lib.helpers import (
    identify_references,
    get_collection_name_to_class_names_map,
)
from refscan.lib.ReferenceList import ReferenceList
from toolz import merge

from nmdc_runtime.api.core.util import sha256hash_from_file
from nmdc_runtime.api.models.object import DrsObjectIn


def get_class_names_from_collection_spec(
    spec: dict, prefix: Optional[str] = None
) -> List[str]:
    """
    Returns the list of classes referenced by the `$ref` values in a JSON Schema snippet describing a collection,
    applying an optional prefix to each class name.

    >>> get_class_names_from_collection_spec({"items": {"foo": "#/$defs/A"}})
    []
    >>> get_class_names_from_collection_spec({"items": {"$ref": "#/$defs/A"}})
    ['A']
    >>> get_class_names_from_collection_spec({"items": {"$ref": "#/$defs/A"}}, "p:")
    ['p:A']
    >>> get_class_names_from_collection_spec({"items": {"anyOf": "not-a-list"}})
    []
    >>> get_class_names_from_collection_spec({"items": {"anyOf": []}})
    []
    >>> get_class_names_from_collection_spec({"items": {"anyOf": [{"$ref": "#/$defs/A"}]}})
    ['A']
    >>> get_class_names_from_collection_spec({"items": {"anyOf": [{"$ref": "#/$defs/A"}, {"$ref": "#/$defs/B"}]}})
    ['A', 'B']
    >>> get_class_names_from_collection_spec({"items": {"anyOf": [{"$ref": "#/$defs/A"}, {"$ref": "#/$defs/B"}]}}, "p:")
    ['p:A', 'p:B']
    """

    class_names = []
    if "items" in spec:
        # If the `items` dictionary has a key named `$ref`, get the single class name from it.
        if "$ref" in spec["items"]:
            ref_dict = spec["items"]["$ref"]
            class_name = ref_dict.split("/")[-1]  # e.g. `#/$defs/Foo` --> `Foo`
            class_names.append(class_name)

        # Else, if it has a key named `anyOf` whose value is a list, get the class name from each ref in the list.
        elif "anyOf" in spec["items"] and isinstance(spec["items"]["anyOf"], list):
            for element in spec["items"]["anyOf"]:
                ref_dict = element["$ref"]
                class_name = ref_dict.split("/")[-1]  # e.g. `#/$defs/Foo` --> `Foo`
                class_names.append(class_name)

    # Apply the specified prefix, if any, to each class name.
    if isinstance(prefix, str):
        class_names = list(map(lambda name: f"{prefix}{name}", class_names))

    return class_names


@lru_cache
def get_allowed_references() -> ReferenceList:
    r"""
    Returns a `ReferenceList` of all the inter-document references that
    the NMDC Schema allows a schema-compliant MongoDB database to contain.
    """

    # Identify the inter-document references that the schema allows a database to contain.
    print("Identifying schema-allowed references.")
    references = identify_references(
        schema_view=nmdc_schema_view(),
        collection_name_to_class_names=collection_name_to_class_names,
    )

    return references


@lru_cache
def get_type_collections() -> dict:
    """Returns a dictionary mapping class names to Mongo collection names."""

    mappings = {}

    # Process the `items` dictionary of each collection whose name ends with `_set`.
    for collection_name, spec in nmdc_jsonschema["properties"].items():
        if collection_name.endswith("_set"):
            class_names = get_class_names_from_collection_spec(spec, "nmdc:")
            for class_name in class_names:
                mappings[class_name] = collection_name

    return mappings


def without_id_patterns(nmdc_jsonschema):
    rv = deepcopy(nmdc_jsonschema)
    for cls_, spec in rv["$defs"].items():
        if "properties" in spec:
            if "id" in spec["properties"]:
                spec["properties"]["id"].pop("pattern", None)
    return rv


@lru_cache
def get_nmdc_jsonschema_dict(enforce_id_patterns=True):
    """Get NMDC JSON Schema with materialized patterns (for identifier regexes)."""
    d = json.loads(
        BytesIO(
            pkgutil.get_data("nmdc_schema", "nmdc_materialized_patterns.schema.json")
        )
        .getvalue()
        .decode("utf-8")
    )
    return d if enforce_id_patterns else without_id_patterns(d)


@lru_cache
def get_nmdc_jsonschema_validator(enforce_id_patterns=True):
    return fastjsonschema.compile(
        get_nmdc_jsonschema_dict(enforce_id_patterns=enforce_id_patterns)
    )


nmdc_jsonschema = get_nmdc_jsonschema_dict()
nmdc_jsonschema_validator = get_nmdc_jsonschema_validator()
nmdc_jsonschema_noidpatterns = get_nmdc_jsonschema_dict(enforce_id_patterns=False)
nmdc_jsonschema_validator_noidpatterns = get_nmdc_jsonschema_validator(
    enforce_id_patterns=False
)

REPO_ROOT_DIR = Path(__file__).parent.parent


def put_object(filepath, url, mime_type=None):
    if mime_type is None:
        mime_type = mimetypes.guess_type(filepath)[0]
    with open(filepath, "rb") as f:
        return requests.put(url, data=f, headers={"Content-Type": mime_type})


def drs_metadata_for(filepath, base=None, timestamp=None):
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
            {"type": "sha256", "checksum": sha256hash_from_file(filepath, timestamp)}
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


def freeze(obj):
    """Recursive function for dict → frozendict, set → frozenset, list → tuple.

    For example, will turn JSON data into a hashable value.
    """
    try:
        # See if the object is hashable
        hash(obj)
        return obj
    except TypeError:
        pass

    if isinstance(obj, (dict, frozendict)):
        return frozendict({k: freeze(obj[k]) for k in obj})
    elif isinstance(obj, (set, frozenset)):
        return frozenset({freeze(elt) for elt in obj})
    elif isinstance(obj, (list, tuple)):
        return tuple([freeze(elt) for elt in obj])

    msg = "Unsupported type: %r" % type(obj).__name__
    raise TypeError(msg)


def unfreeze(obj):
    """frozendict → dict, frozenset → set, tuple → list."""
    if isinstance(obj, (dict, frozendict)):
        return {k: unfreeze(v) for k, v in obj.items()}
    elif isinstance(obj, (set, frozenset)):
        return {unfreeze(elt) for elt in obj}
    elif isinstance(obj, (list, tuple)):
        return [unfreeze(elt) for elt in obj]
    else:
        return obj


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


def flatten(d):
    """Flatten a nested JSON-able dict into a flat dict of dotted-pathed keys."""
    # assumes plain-json-able
    d = json.loads(json.dumps(d))

    # atomic values are already "flattened"
    if not isinstance(d, (dict, list)):
        return d

    out = {}
    for k, v in d.items():
        if isinstance(v, list):
            for i, elt in enumerate(v):
                if isinstance(elt, dict):
                    for k_inner, v_inner in flatten(elt).items():
                        out[f"{k}.{i}.{k_inner}"] = v_inner
                elif isinstance(elt, list):
                    raise ValueError("Can't handle lists in lists at this time")
                else:
                    out[f"{k}.{i}"] = elt
        elif isinstance(v, dict):
            for kv, vv in v.items():
                if isinstance(vv, dict):
                    for kv_inner, vv_inner in flatten(vv).items():
                        out[f"{k}.{kv}.{kv_inner}"] = vv_inner
                elif isinstance(vv, list):
                    raise ValueError("Can't handle lists in sub-dicts at this time")
                else:
                    out[f"{k}.{kv}"] = vv
        else:
            out[k] = v
    return out


def find_one(k_v: dict, entities: Iterable[dict]):
    """Find the first entity with key-value pair k_v, if any?

    >>> find_one({"id": "foo"}, [{"id": "foo"}])
    {'id': 'foo'}
    >>> find_one({"id": "foo"}, [{"id": "bar"}]) is None
    True
    """
    if len(k_v) > 1:
        raise Exception("Supports only one key-value pair")
    k = next(k for k in k_v)
    return next((e for e in entities if k in e and e[k] == k_v[k]), None)


@lru_cache
def nmdc_activity_collection_names():
    slots = []
    view = ViewGetter().get_view()
    acts = set(view.class_descendants("WorkflowExecutionActivity"))
    acts -= {"WorkflowExecutionActivity"}
    for slot in view.class_slots("Database"):
        rng = getattr(view.get_slot(slot), "range", None)
        if rng in acts:
            slots.append(slot)
    return slots


@lru_cache
def nmdc_schema_view():
    return ViewGetter().get_view()


@lru_cache
def get_class_name_to_collection_names_map(
    schema_view: SchemaView,
) -> Dict[str, List[str]]:
    """
    Returns a mapping of class names to the names of the collections that can store instances of those classes/types,
    according to the specified `SchemaView`.

    Example output:
    ```
    {
        "Study": ["study_set"],
        "Biosample": ["biosample_set"],
        ...
    }
    ```
    """
    class_name_to_collection_names = defaultdict(list)
    for collection_name, class_names in get_collection_name_to_class_names_map(
        schema_view
    ).items():
        for class_name in class_names:
            class_name_to_collection_names[class_name].append(collection_name)
    return class_name_to_collection_names


@lru_cache
def nmdc_database_collection_instance_class_names():
    names = []
    view = nmdc_schema_view()
    all_classes = set(view.all_classes())
    for slot in view.class_slots("Database"):
        rng = getattr(view.get_slot(slot), "range", None)
        if rng in all_classes:
            names.append(rng)
    return names


@lru_cache
def nmdc_database_collection_names():
    r"""
    TODO: Document this function.

    TODO: Assuming this function was designed to return a list of names of all Database slots that represents database
          collections, import/use the function named `get_collection_names_from_schema` from `refscan.lib.helpers`
          instead, since (a) it includes documentation and (b) it performs the additional checks the lead schema
          maintainer expects (e.g. checking whether a slot is `multivalued` and `inlined_as_list`).
    """
    names = []
    view = nmdc_schema_view()
    all_classes = set(view.all_classes())
    for slot in view.class_slots("Database"):
        rng = getattr(view.get_slot(slot), "range", None)
        if rng in all_classes:
            names.append(slot)
    return names


def all_docs_have_unique_id(coll) -> bool:
    first_doc = coll.find_one({}, ["id"])
    if first_doc is None or "id" not in first_doc:
        # short-circuit exit for empty collection or large collection via first-doc peek.
        return False

    total_count = coll.count_documents({})
    return (
        # avoid attempt to fetch large (>16mb) list of distinct IDs,
        # a limitation of collection.distinct(). Use aggregation pipeline
        # instead to compute on mongo server, using disk if necessary.
        next(
            coll.aggregate(
                [{"$group": {"_id": "$id"}}, {"$count": "n_unique_ids"}],
                allowDiskUse=True,
            )
        )["n_unique_ids"]
        == total_count
    )


def specialize_activity_set_docs(docs):
    """
    TODO: Document this function.

    TODO: Check whether this function is still necessary, given that the `Database` class
          in `nmdc-schema` does not have a slot named `activity_set`.
    """
    validation_errors = {}
    type_collections = get_type_collections()
    if "activity_set" in docs:
        for doc in docs["activity_set"]:
            doc_type = doc["type"]
            try:
                collection_name = type_collections[doc_type]
            except KeyError:
                msg = (
                    f"activity_set doc {doc.get('id', '<id missing>')} "
                    f"has type {doc_type}, which is not in NMDC Schema. "
                    "Note: Case is sensitive."
                )
                if "activity_set" in validation_errors:
                    validation_errors["activity_set"].append(msg)
                else:
                    validation_errors["activity_set"] = [msg]
                continue

            if collection_name in docs:
                docs[collection_name].append(doc)
            else:
                docs[collection_name] = [doc]
        del docs["activity_set"]
    return docs, validation_errors


# Define a mapping from collection name to a list of class names allowable for that collection's documents.
collection_name_to_class_names: Dict[str, List[str]] = {
    collection_name: list(
        set(
            chain.from_iterable(
                nmdc_schema_view().class_descendants(cls_name)
                for cls_name in get_class_names_from_collection_spec(spec)
            )
        )
    )
    for collection_name, spec in nmdc_jsonschema["$defs"]["Database"][
        "properties"
    ].items()
}


def class_hierarchy_as_list(obj) -> list[str]:
    """
    get list of inherited classes for each concrete class
    """
    rv = []
    current_class = obj.__class__

    def recurse_through_bases(cls):
        if cls.__name__ == "YAMLRoot":
            return rv
        rv.append(cls.__name__)
        for base in cls.__bases__:
            recurse_through_bases(base)
        return rv

    return recurse_through_bases(current_class)


@lru_cache
def schema_collection_names_with_id_field() -> Set[str]:
    """
    Returns the set of collection names with which _any_ of the associated classes contains an `id` field.
    """

    target_collection_names = set()

    for collection_name, class_names in collection_name_to_class_names.items():
        for class_name in class_names:
            if "id" in nmdc_jsonschema["$defs"][class_name].get("properties", {}):
                target_collection_names.add(collection_name)
                break

    return target_collection_names


def populated_schema_collection_names_with_id_field(mdb: MongoDatabase) -> List[str]:
    collection_names = sorted(schema_collection_names_with_id_field())
    return [n for n in collection_names if mdb[n].find_one({"id": {"$exists": True}})]


def does_collection_have_unique_index_on_id_field(
    collection_name: str, db: MongoDatabase
) -> bool:
    """Check whether the specified MongoDB collection has a unique index on its `id` field (not `_id`).

    Note: If the specified MongoDB collection either does not exist or is a _view_ instead of a collection,
          this function will return `False`.

    References:
    - https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.list_indexes
    - https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.index_information
    """
    # Check whether the specified collection actually exists in the database; and, if it does,
    # whether it is really a _collection_ (as opposed to being a _view_). If it doesn't exist,
    # or it is a view, return `False` right away.
    collection_infos_cursor = db.list_collections(filter={"name": collection_name})
    collection_infos = list(collection_infos_cursor)
    if len(collection_infos) == 0:
        return False
    collection_info = collection_infos[0]
    if collection_info["type"] != "collection":
        return False

    # Now that we know we're dealing with a collection, get information about each of its indexes.
    collection = db.get_collection(collection_name)
    for index_information in collection.list_indexes():
        # Get the "field_name-direction" pairs that make up this index.
        field_name_and_direction_pairs: SON = index_information["key"]

        # If this index involves a number of fields other than one, skip it.
        # We're only interested in indexes that involve the `id` field by itself.
        if len(field_name_and_direction_pairs.keys()) != 1:
            continue

        # Check whether the field this index involves is the `id` field,
        # and whether this index is `unique`.
        field_name = list(field_name_and_direction_pairs.keys())[0]
        if field_name == "id" and index_information.get("unique", False):
            return True

    return False


def ensure_unique_id_indexes(mdb: MongoDatabase):
    """Ensure that any collections with an "id" field have an index on "id"."""

    # Note: The pipe (i.e. `|`) operator performs a union of the two sets. In this case,
    #       it creates a set (i.e. `candidate_names`) consisting of the names of both
    #       (a) all collections in the real database, and (b) all collections that
    #       the NMDC schema says can contain instances of classes that have an "id" slot.
    candidate_names = (
        set(mdb.list_collection_names()) | schema_collection_names_with_id_field()
    )
    for collection_name in candidate_names:
        if collection_name.startswith("system."):  # reserved by mongodb
            continue

        # If the collection already has a unique index on `id`, there's no need
        # to check anything else about the collection.
        if does_collection_have_unique_index_on_id_field(collection_name, mdb):
            continue

        if (
            collection_name in schema_collection_names_with_id_field()
            or all_docs_have_unique_id(mdb[collection_name])
        ):
            # Check if index already exists, and if so, drop it if not unique
            try:
                existing_indexes = list(mdb[collection_name].list_indexes())
                id_index = next(
                    (idx for idx in existing_indexes if idx["name"] == "id_1"), None
                )

                if id_index:
                    # If index exists but isn't unique, drop it so we can recreate
                    if not id_index.get("unique", False):
                        mdb[collection_name].drop_index("id_1")

                # Create index with unique constraint
                mdb[collection_name].create_index("id", unique=True)
            except OperationFailure as e:
                # If error is about index with same name, just continue
                if "An existing index has the same name" in str(e):
                    continue
                else:
                    # Re-raise other errors
                    raise


def decorate_if(condition: bool = False) -> Callable:
    r"""
    Decorator that applies another decorator only when `condition` is `True`.

    Note: We implemented this so we could conditionally register
          endpoints with FastAPI's `@router`.

    Example usages:
    A. Apply the `@router.get` decorator:
       ```python
       @decorate_if(True)(router.get("/me"))
       def get_me(...):
           ...
       ```
    B. Bypass the `@router.get` decorator:
       ```python
       @decorate_if(False)(router.get("/me"))
       def get_me(...):
           ...
       ```
    """

    def apply_original_decorator(original_decorator: Callable) -> Callable:
        def check_condition(original_function: Callable) -> Callable:
            if condition:
                return original_decorator(original_function)
            else:
                return original_function

        return check_condition

    return apply_original_decorator
