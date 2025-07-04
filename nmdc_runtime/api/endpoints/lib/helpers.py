import json
import bson.json_util
from typing import List

from pymongo.database import Database
from refscan.lib.Finder import Finder
from refscan.lib.helpers import derive_schema_class_name_from_document
from refscan.scanner import identify_referring_documents, scan_outgoing_references

from nmdc_runtime.api.models.lib.helpers import derive_update_specs
from nmdc_runtime.api.models.query import UpdateCommand, UpdateSpecs
from nmdc_runtime.util import get_allowed_references, nmdc_schema_view


def simulate_updates_and_check_references(
    db: Database, update_cmd: UpdateCommand
) -> List[str]:
    r"""
    Checks whether—if we were to perform the specified updates—each
    of the following things would be true after the updates were performed:
    1. Outgoing references: The documents that were _updated_ do not contain any
       broken references (i.e. all the documents to which they contain references,
       _exist_ in collections allowed by the schema).
    2. Incoming references: The documents that originally _referenced_ any documents
       that were updated do not contain any broken references. This is necessary
       because update operations can currently change `id` and `type` values.

    This function does that by performing the updates within a MongoDB transaction,
    leaving the transaction in the _pending_ (i.e. not committed) state, and then
    performing various checks on the database in that tentative state.

    :param db: The database on which to simulate performing the updates
    :param update_cmd: The command that specifies the updates

    :return: List of violation messages. If the list is empty, it means that—if
             the updates had been performed (instead of only simulated) here—they
             would not have left behind any broken references.
    """

    # Initialize the list of violation messages that we will return.
    violation_messages: List[str] = []

    # Instantiate a `Finder` bound to the Mongo database. This will be
    # used later, to identify and check inter-document references.
    finder = Finder(database=db)

    # Extract the collection name from the command.
    collection_name = update_cmd.update

    # Derive the update specifications from the command.
    update_specs: UpdateSpecs = derive_update_specs(update_cmd)

    # Get a reference to a `SchemaView` bound to the NMDC schema, so we can
    # use it to, for example, map `type` field values to schema class names.
    schema_view = nmdc_schema_view()

    # Get some data structures that indicate which fields of which documents
    # can legally contain references, according to the NMDC schema.
    legal_references = get_allowed_references()
    reference_field_names_by_source_class_name = (
        legal_references.get_reference_field_names_by_source_class_name()
    )

    # Start a "throwaway" MongoDB transaction so we can simulate the updates.
    with db.client.start_session() as session:
        with session.start_transaction():

            # Make a list of the `_id`, `id`, and `type` values of the documents that
            # the user wants to update.
            projection = {"_id": 1, "id": 1, "type": 1}
            subject_document_descriptors = list(
                db[collection_name].find(
                    filter={"$or": [spec["filter"] for spec in update_specs]},
                    projection=projection,
                    session=session,
                )
            )

            # Make a set of the `_id` values of the subject documents so that (later) we can
            # check whether a given _referring_ document is also one of the _subject_
            # documents (i.e. is among the documents the user wants to update).
            subject_document_object_ids = set(
                tdd["_id"] for tdd in subject_document_descriptors
            )

            # Identify _all_ documents that reference any of the subject documents.
            all_referring_document_descriptors_pre_update = []
            for subject_document_descriptor in subject_document_descriptors:
                # If the document descriptor lacks the "id" field, we already know that no
                # documents reference it (since they would have to _use_ that "id" value to
                # do so). So, we don't bother trying to identify documents that reference it.
                if "id" not in subject_document_descriptor:
                    continue

                referring_document_descriptors = identify_referring_documents(
                    document=subject_document_descriptor,  # expects at least "id" and "type"
                    schema_view=schema_view,
                    references=legal_references,
                    finder=finder,
                    client_session=session,
                )
                all_referring_document_descriptors_pre_update.extend(
                    referring_document_descriptors
                )

            # Simulate the updates (i.e. apply them within the context of the transaction).
            db.command(
                # Note: This expression was copied from the `_run_mdb_cmd` function in `queries.py`.
                # TODO: Document this expression (i.e. the Pydantic->JSON->BSON chain).
                bson.json_util.loads(
                    json.dumps(update_cmd.model_dump(exclude_unset=True))
                ),
                session=session,
            )
            # For each referring document, check whether any of its outgoing references
            # is broken (in the context of the transaction).
            for descriptor in all_referring_document_descriptors_pre_update:
                referring_document_oid = descriptor["source_document_object_id"]
                referring_document_id = descriptor["source_document_id"]
                referring_collection_name = descriptor["source_collection_name"]
                # If the referring document is among the documents that the user wanted to
                # update, we skip it for now. We will check its outgoing references later
                # (i.e. when we check the outgoing references of _all_ updated documents).
                if referring_document_oid in subject_document_object_ids:
                    continue
                # Get the referring document, so we can check its outgoing references.
                # Note: We project only the fields that can legally contain references,
                #       plus other fields involved in referential integrity checking.
                referring_document_reference_field_names = (
                    reference_field_names_by_source_class_name[
                        descriptor["source_class_name"]
                    ]
                )
                projection = {
                    field_name: 1
                    for field_name in referring_document_reference_field_names
                } | {
                    "_id": 1,
                    "id": 1,
                    "type": 1,
                }  # note: `|` unions the dicts
                referring_document = db[referring_collection_name].find_one(
                    {"_id": referring_document_oid},
                    projection=projection,
                    session=session,
                )
                # Note: We assert that the referring document exists (to satisfy the type checker).
                assert (
                    referring_document is not None
                ), "A referring document has vanished."
                violations = scan_outgoing_references(
                    document=referring_document,
                    source_collection_name=referring_collection_name,
                    schema_view=schema_view,
                    references=legal_references,
                    finder=finder,
                    client_session=session,  # so it uses the pending transaction's session
                )
                # For each violation (i.e. broken reference) that exists, add a violation message
                # to the list of violation messages.
                #
                # TODO: The violation might not involve a reference to one of the
                #       subject documents. The `scan_outgoing_references` function
                #       scans _all_ references emanating from the document.
                #
                for violation in violations:
                    source_field_name = violation.source_field_name
                    target_id = violation.target_id
                    violation_messages.append(
                        (
                            f"The document having 'id'='{referring_document_id}' in "
                            f"the collection '{referring_collection_name}' contains a "
                            f"reference (in its '{source_field_name}' field, "
                            f"referring to the document having id='{target_id}') "
                            f"which would be broken."
                        )
                    )

            # For each updated document, check whether any of its outgoing references
            # is broken (in the context of the transaction).
            for descriptor in subject_document_descriptors:
                updated_document_oid = descriptor["_id"]
                updated_document_id = descriptor["id"]
                updated_document_class_name = derive_schema_class_name_from_document(
                    document=descriptor,
                    schema_view=schema_view,
                )
                assert (
                    updated_document_class_name is not None
                ), "The updated document does not represent a valid schema class instance."
                updated_collection_name = (
                    collection_name  # makes a disambiguating alias
                )
                # Get the updated document, so we can check its outgoing references.
                # Note: We project only the fields that can legally contain references,
                #       plus other fields involved in referential integrity checking.
                updated_document_reference_field_names = (
                    reference_field_names_by_source_class_name[
                        updated_document_class_name
                    ]
                )
                projection = {
                    field_name: 1
                    for field_name in updated_document_reference_field_names
                } | {
                    "_id": 1,
                    "id": 1,
                    "type": 1,
                }  # note: `|` unions the dicts
                updated_document = db[updated_collection_name].find_one(
                    {"_id": updated_document_oid},
                    projection=projection,
                    session=session,
                )
                # Note: We assert that the updated document exists (to satisfy the type checker).
                assert updated_document is not None, "An updated document has vanished."
                violations = scan_outgoing_references(
                    document=updated_document,
                    source_collection_name=updated_collection_name,
                    schema_view=schema_view,
                    references=legal_references,
                    finder=finder,
                    client_session=session,  # so it uses the pending transaction's session
                )
                # For each violation (i.e. broken reference) that exists, add a violation message
                # to the list of violation messages.
                for violation in violations:
                    source_field_name = violation.source_field_name
                    target_id = violation.target_id
                    violation_messages.append(
                        (
                            f"The document having 'id'='{updated_document_id}' in "
                            f"the collection '{updated_collection_name}' contains a "
                            f"reference (in its '{source_field_name}' field, "
                            f"referring to the document having id='{target_id}') "
                            f"which would be broken."
                        )
                    )

            # Whatever happens (i.e. whether there are violations or not), abort the transaction.
            #
            # Note: If an exception was raised within this `with` block, the transaction
            #       will already have been aborted automatically (and execution will not
            #       have reached this statement). On the other hand, if no exception
            #       was raised, we explicitly abort the transaction so that the updates
            #       that we "simulated" in this block do not get applied to the real database.
            #       Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html
            #
            session.abort_transaction()

    return violation_messages
