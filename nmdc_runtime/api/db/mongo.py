import gzip
import os
from contextlib import AbstractContextManager
from copy import deepcopy
from functools import lru_cache
from typing import Set
from uuid import uuid4

import bson
from nmdc_schema.nmdc import Database as NMDCDatabase
from pymongo.errors import AutoReconnect, OperationFailure
from refscan.lib.Finder import Finder
from refscan.scanner import scan_outgoing_references
from tenacity import wait_random_exponential, retry, retry_if_exception_type
from toolz import merge, unique
from refscan.lib.helpers import get_collection_names_from_schema

from nmdc_runtime.api.models.query import UpdateStatement, DeleteStatement
from nmdc_runtime.mongo_util import SessionBoundDatabase
from nmdc_runtime.util import (
    nmdc_schema_view,
    collection_name_to_class_names,
    ensure_unique_id_indexes,
    nmdc_database_collection_names,
    get_allowed_references,
    get_nmdc_schema_validator,
)
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase


@retry(
    retry=retry_if_exception_type(AutoReconnect),
    wait=wait_random_exponential(multiplier=0.5, max=60),
)
def check_mongo_ok_autoreconnect(mdb: MongoDatabase):
    r"""
    Check whether the application can write to the database.
    """
    collection = mdb.get_collection("_runtime.healthcheck")
    collection.insert_one({"status": "ok"})
    collection.delete_many({"status": "ok"})
    return True


@lru_cache
def get_mongo_client() -> MongoClient:
    r"""
    Returns a `MongoClient` instance you can use to access the MongoDB server specified via environment variables.
    Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
    """
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    return MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=username if username else None,
        password=password if password else None,
        directConnection=True,
    )


@lru_cache
def get_mongo_db() -> MongoDatabase:
    r"""
    Returns a `Database` instance you can use to access the MongoDB database specified via an environment variable.
    Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/database.html#pymongo.database.Database
    """
    _client = get_mongo_client()
    mdb = _client[os.getenv("MONGO_DBNAME")]
    check_mongo_ok_autoreconnect(mdb)
    return mdb


@lru_cache
def get_session_bound_mongo_db(session=None) -> MongoDatabase:
    r"""
    Returns a `Database` instance you can use to access the MongoDB database specified via an environment variable.
    Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/database.html#pymongo.database.Database
    """
    _client = get_mongo_client()
    mdb = _client[os.getenv("MONGO_DBNAME")]
    check_mongo_ok_autoreconnect(mdb)
    return SessionBoundDatabase(mdb, session) if session is not None else mdb


def get_nonempty_nmdc_schema_collection_names(mdb: MongoDatabase) -> Set[str]:
    """
    Returns the names of the collections that (a) exist in the database,
    (b) are described by the schema, and (c) contain at least one document.

    Note: The ampersand (`&`) is the "set intersection" operator.
    """
    collection_names_from_database = mdb.list_collection_names()
    schema_view = nmdc_schema_view()
    collection_names_from_schema = get_collection_names_from_schema(schema_view)
    names = set(collection_names_from_database) & set(collection_names_from_schema)
    return {name for name in names if mdb[name].estimated_document_count() > 0}


@lru_cache
def activity_collection_names(mdb: MongoDatabase) -> Set[str]:
    r"""
    TODO: Document this function.
    """
    return get_nonempty_nmdc_schema_collection_names(mdb) - {
        "biosample_set",
        "study_set",
        "data_object_set",
        "functional_annotation_set",
        "genome_feature_set",
    }


@lru_cache
def get_planned_process_collection_names() -> Set[str]:
    r"""
    Returns the names of all collections that the schema says can contain documents
    that represent instances of the `PlannedProcess` class or any of its subclasses.
    """
    schema_view = nmdc_schema_view()
    collection_names = set()
    planned_process_descendants = set(schema_view.class_descendants("PlannedProcess"))

    for collection_name, class_names in collection_name_to_class_names.items():
        for class_name in class_names:
            # If the name of this class is the name of the `PlannedProcess` class
            # or any of its subclasses, add it to the result set.
            if class_name in planned_process_descendants:
                collection_names.add(collection_name)

    return collection_names


def mongodump_excluded_collections() -> str:
    """
    TODO: Document this function.
    """
    _mdb = get_mongo_db()
    schema_view = nmdc_schema_view()
    collection_names_from_database = _mdb.list_collection_names()
    collection_names_from_schema = get_collection_names_from_schema(schema_view)
    excluded_collections = " ".join(
        f"--excludeCollection={c}"
        for c in sorted(
            set(collection_names_from_database) - set(collection_names_from_schema)
        )
    )
    return excluded_collections


def mongorestore_collection(mdb, collection_name, bson_file_path):
    """
    Replaces the specified collection with one that reflects the contents of the
    specified BSON file.
    """
    with gzip.open(bson_file_path, "rb") as bson_file:
        data = bson.decode_all(bson_file.read())
        if data:
            mdb.drop_collection(collection_name)
            mdb[collection_name].insert_many(data)
            print(
                f"mongorestore_collection: inserted {len(data)} documents into {collection_name} after drop"
            )
        else:
            print(f"mongorestore_collection: no {collection_name} documents found")


def mongorestore_from_dir(mdb, dump_directory, skip_collections=None):
    """
    Effectively runs a `mongorestore` command in pure Python.
    Helpful in a container context that does not have the `mongorestore` command available.
    """
    skip_collections = skip_collections or []
    for root, dirs, files in os.walk(dump_directory):
        for file in files:
            if file.endswith(".bson.gz"):
                collection_name = file.replace(".bson.gz", "")
                if collection_name in skip_collections:
                    continue
                bson_file_path = os.path.join(root, file)
                mongorestore_collection(mdb, collection_name, bson_file_path)

    print("mongorestore_from_dir completed successfully.")


class OverlayDBError(Exception):
    pass


class OverlayDB(AbstractContextManager):
    """Provides a context whereby a base Database is overlaid with a temporary one.

    If you need to run basic simulations of updates to a base database,
    you don't want to actually commit transactions to the base database.

    For example, to insert or replace (matching on "id") many documents into a collection in order
    to then validate the resulting total set of collection documents, an OverlayDB writes to
    an overlay collection that "shadows" the base collection during a "find" query
    (the "merge_find" method of an OverlayDB object): if a document with `id0` is found in the
    overlay collection, that id is marked as "seen" and will not also be returned when
    subsequently scanning the (unmodified) base-database collection.

    Note: The OverlayDB object does not provide a means to perform arbitrary MongoDB queries on the virtual "merged"
          database. Callers can access the real database via `overlay_db._bottom_db` and the overlaying database via
          `overlay_db._top_db` and perform arbitrary MongoDB queries on the individual databases that way. Access to
          the virtual "merged" database is limited to the methods of the `OverlayDB` class, which simulates the
          "merging" just-in-time to process the method invocation. You can see an example of this in the implementation
          of the `merge_find` method, which internally accesses both the real database and the overlaying database.

    Mongo "update" commands (as the "apply_updates" method) are simulated by first copying affected
    documents from a base collection to the overlay, and then applying the updates to the overlay,
    so that again, base collections are unmodified, and a "merge_find" call will produce a result
    *as if* the base collection(s) were modified.

    Mongo deletions (as the "delete" method) also copy affected documents from the base collection
    to the overlay collection, and flag them using the "_deleted" field. In this way, a `merge_find`
    call will match a relevant document given a suitable filter, and will mark the document's id
    as "seen" *without* returning the document. Thus, the result is as if the document were deleted.

    Usage:
    ````
    with OverlayDB(mdb) as odb:
        # do stuff, e.g. `odb.replace_or_insert_many(...)`
    ```
    """

    def __init__(self, mdb: MongoDatabase):
        self._bottom_db = mdb
        self._top_db = self._bottom_db.client.get_database(f"overlay-{uuid4()}")
        ensure_unique_id_indexes(self._top_db)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._bottom_db.client.drop_database(self._top_db.name)

    def replace_or_insert_many(self, coll_name, documents: list):
        try:
            self._top_db[coll_name].insert_many(documents)
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def apply_updates(self, coll_name, updates: list):
        """prepare overlay db and apply updates to it."""
        assert all(UpdateStatement(**us) for us in updates)
        for update_spec in updates:
            for bottom_doc in self._bottom_db[coll_name].find(update_spec["q"]):
                self._top_db[coll_name].insert_one(bottom_doc)
        try:
            self._top_db.command({"update": coll_name, "updates": updates})
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def delete(self, coll_name, deletes: list):
        """ "apply" delete command by flagging docs in overlay database"""
        assert all(DeleteStatement(**us) for us in deletes)
        for delete_spec in deletes:
            for bottom_doc in self._bottom_db[coll_name].find(
                delete_spec["q"], limit=delete_spec["limit"]
            ):
                bottom_doc["_deleted"] = True
                self._top_db[coll_name].insert_one(bottom_doc)

    def merge_find(self, coll_name, find_spec: dict):
        """Yield docs first from overlay and then from base db, minding deletion flags."""
        # ensure projection of "id" and "_deleted"
        if "projection" in find_spec:
            proj = find_spec["projection"]
            if isinstance(proj, dict):
                proj = merge(proj, {"id": 1, "_deleted": 1})
            elif isinstance(proj, list):
                proj = list(unique(proj + ["id", "_deleted"]))

        top_docs = self._top_db[coll_name].find(**find_spec)
        bottom_docs = self._bottom_db[coll_name].find(**find_spec)
        top_seen_ids = set()
        for doc in top_docs:
            if not doc.get("_deleted"):
                yield doc
            top_seen_ids.add(doc["id"])

        for doc in bottom_docs:
            if doc["id"] not in top_seen_ids:
                yield doc


def validate_json(
    in_docs: dict, mdb: MongoDatabase, check_inter_document_references: bool = False
):
    r"""
    Checks whether the specified dictionary represents a valid instance of the `Database` class
    defined in the NMDC Schema. Referential integrity checking is performed on an opt-in basis.

    Example dictionary:
    {
        "biosample_set": [
            {"id": "nmdc:bsm-00-000001", ...},
            {"id": "nmdc:bsm-00-000002", ...}
        ],
        "study_set": [
            {"id": "nmdc:sty-00-000001", ...},
            {"id": "nmdc:sty-00-000002", ...}
        ]
    }

    :param in_docs: The dictionary you want to validate
    :param mdb: A reference to a MongoDB database
    :param check_inter_document_references: Whether you want this function to check whether every document that
                                            is referenced by any of the documents passed in would, indeed, exist
                                            in the database, if the documents passed in were to be inserted into
                                            the database. In other words, set this to `True` if you want this
                                            function to perform referential integrity checks.
    """
    validator = get_nmdc_schema_validator()
    docs = deepcopy(in_docs)
    validation_errors = {}

    known_coll_names = set(nmdc_database_collection_names())
    for coll_name, coll_docs in docs.items():
        if coll_name not in known_coll_names:
            # We expect each key in `in_docs` to be a known schema collection name. However, `@type` is a special key
            # for JSON-LD, used for JSON serialization of e.g. LinkML objects. That is, the value of `@type` lets a
            # client know that the JSON object (a dict in Python) should be interpreted as a
            # <https://w3id.org/nmdc/Database>. If `@type` is present as a key, and its value indicates that
            # `in_docs` is indeed a nmdc:Database, that's fine, and we don't want to raise an exception.
            #
            # prompted by: https://github.com/microbiomedata/nmdc-runtime/discussions/858
            if coll_name == "@type" and coll_docs in ("Database", "nmdc:Database"):
                continue
            else:
                validation_errors[coll_name] = [
                    f"'{coll_name}' is not a known schema collection name"
                ]
                continue

        errors = list(
            validator.iter_results({coll_name: coll_docs}, target_class="Database")
        )
        validation_errors[coll_name] = [e.message for e in errors]
        if coll_docs:
            if not isinstance(coll_docs, list):
                validation_errors[coll_name].append("value must be a list")
            elif not all(isinstance(d, dict) for d in coll_docs):
                validation_errors[coll_name].append(
                    "all elements of list must be dicts"
                )
            if not validation_errors[coll_name]:
                try:
                    with OverlayDB(mdb) as odb:
                        odb.replace_or_insert_many(coll_name, coll_docs)
                except OverlayDBError as e:
                    validation_errors[coll_name].append(str(e))

    if all(len(v) == 0 for v in validation_errors.values()):
        # Second pass. Try instantiating linkml-sourced dataclass
        in_docs.pop("@type", None)
        try:
            NMDCDatabase(**in_docs)
        except Exception as e:
            return {"result": "errors", "detail": str(e)}

        # Third pass (if enabled): Check inter-document references.
        if check_inter_document_references is True:
            # Prepare to use `refscan`.
            #
            # Note: We check the inter-document references in two stages, which are:
            #       1. For each document in the JSON payload, check whether each document it references already exists
            #          (in the collections the schema says it can exist in) in the database. We use the
            #          `refscan` package to do this, which returns violation details we'll use in the second stage.
            #       2. For each violation found in the first stage (i.e. each reference to a not-found document), we
            #          check whether that document exists (in the collections the schema says it can exist in) in the
            #          JSON payload. If it does, then we "waive" (i.e. discard) that violation.
            #       The violations that remain after those two stages are the ones we return to the caller.
            #
            # Note: The reason we do not insert documents into an `OverlayDB` and scan _that_, is that the `OverlayDB`
            #       does not provide a means to perform arbitrary queries against its virtual "merged" database. It
            #       is not a drop-in replacement for a pymongo's `Database` class, which is the only thing that
            #       `refscan`'s `Finder` class accepts.
            #
            finder = Finder(database=mdb)
            references = get_allowed_references()

            # Iterate over the collections in the JSON payload.
            for source_collection_name, documents in in_docs.items():
                for document in documents:
                    # Add an `_id` field to the document, since `refscan` requires the document to have one.
                    source_document = dict(document, _id=None)
                    violations = scan_outgoing_references(
                        document=source_document,
                        schema_view=nmdc_schema_view(),
                        references=references,
                        finder=finder,
                        source_collection_name=source_collection_name,
                        user_wants_to_locate_misplaced_documents=False,
                    )

                    # For each violation, check whether the misplaced document is in the JSON payload, itself.
                    for violation in violations:
                        can_waive_violation = False
                        # Determine which collections can contain the referenced document, based upon
                        # the schema class of which this source document is an instance.
                        target_collection_names = (
                            references.get_target_collection_names(
                                source_class_name=violation.source_class_name,
                                source_field_name=violation.source_field_name,
                            )
                        )
                        # Check whether the referenced document exists in any of those collections in the JSON payload.
                        for json_coll_name, json_coll_docs in in_docs.items():
                            if json_coll_name in target_collection_names:
                                for json_coll_doc in json_coll_docs:
                                    if json_coll_doc["id"] == violation.target_id:
                                        can_waive_violation = True
                                        break  # stop checking
                            if can_waive_violation:
                                break  # stop checking
                        if not can_waive_violation:
                            violation_as_str = (
                                f"Document '{violation.source_document_id}' "
                                f"in collection '{violation.source_collection_name}' "
                                f"has a field '{violation.source_field_name}' that "
                                f"references a document having id "
                                f"'{violation.target_id}', but the latter document "
                                f"does not exist in any of the collections the "
                                f"NMDC Schema says it can exist in."
                            )
                            validation_errors[source_collection_name].append(
                                violation_as_str
                            )

            # If any collection's error list is not empty, return an error response.
            if any(len(v) > 0 for v in validation_errors.values()):
                return {"result": "errors", "detail": validation_errors}

        return {"result": "All Okay!"}
    else:
        return {"result": "errors", "detail": validation_errors}
