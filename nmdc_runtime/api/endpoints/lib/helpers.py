import json
import bson.json_util

from fastapi import status, HTTPException
from pymongo.database import Database
from refscan.lib.Finder import Finder
from refscan.scanner import identify_referring_documents, scan_outgoing_references

from nmdc_runtime.api.models.lib.helpers import derive_update_specs
from nmdc_runtime.api.models.query import UpdateCommand, UpdateSpecs
from nmdc_runtime.util import get_allowed_references, nmdc_schema_view


def simulate_updates_and_check_references(
    db: Database, update_cmd: UpdateCommand
) -> None:
    r"""
    Checks whether—if we were to perform the specified updates—each
    of the following things would be true after the updates were performed:
    1. Outgoing references: The documents that were updated do not contain any
       broken references (i.e. all the documents they contain references to,
       exist in collections allowed by the schema).
    2. Incoming references: The documents that referenced any documents that
       were updated do not contain any broken references. This is necessary
       because update operations can currently change `id` and `type` values.

    This function does that by performing the updates within a MongoDB transaction,
    leaving the transaction in the _pending_ (i.e. not committed) state, and then
    performing various checks on the database in that tentative state.

    :param db: The database on which to simulate performing the updates
    :param update_cmd: The command that specifies the updates
    """

    finder = Finder(database=db)

    # Extract the collection name from the command.
    collection_name = update_cmd.update

    # Derive the update specifications from the command.
    update_specs: UpdateSpecs = derive_update_specs(update_cmd)

    # Start a "throwaway" MongoDB transaction so we can simulate the updates.
    with db.client.start_session() as session:
        with session.start_transaction():

            # Make a list of the `_id`, `id`, and `type` values of the documents that
            # the user wants to update.
            target_document_descriptors = list(
                db[collection_name].find(
                    filter={"$or": [spec["filter"] for spec in update_specs]},
                    projection={"_id": 1, "id": 1, "type": 1},
                    session=session,
                )
            )

            # Make a set of the `_id` values of the target documents so that (later) we can
            # check whether a given _referring_ document is also one of the _target_ documents
            # (i.e. is among the documents the user wants to update).
            target_document_object_ids = set(
                tdd["_id"] for tdd in target_document_descriptors
            )

            # Identify all documents that reference any of the target documents.
            all_referring_document_descriptors_pre_update = []
            for target_document_descriptor in target_document_descriptors:
                # If the document descriptor lacks the "id" field, we already know that no
                # documents reference it (since they would have to _use_ that "id" value to
                # do so). So, we don't bother trying to identify documents that reference it.
                if "id" not in target_document_descriptor:
                    continue

                referring_document_descriptors = identify_referring_documents(
                    document=target_document_descriptor,  # expects at least "id" and "type"
                    schema_view=nmdc_schema_view(),
                    references=get_allowed_references(),
                    finder=finder,
                    client_session=session,
                )
                all_referring_document_descriptors_pre_update.extend(
                    referring_document_descriptors
                )

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
                # If the referring document is among the documents that the user wants to
                # update, we skip it, since we will check its outgoing references later.
                if referring_document_oid in target_document_object_ids:
                    continue
                # Get the referring document, so we can check its outgoing references.
                # TODO: Consider projecting only necessary fields.
                referring_document = db[referring_collection_name].find_one(
                    {"_id": referring_document_oid},
                    session=session,
                )
                # If the referring document is not found, we skip it, since it means that it
                # was deleted since we identified it.
                if referring_document is None:
                    continue
                violations = scan_outgoing_references(
                    document=referring_document,
                    source_collection_name=referring_collection_name,
                    schema_view=nmdc_schema_view(),
                    references=get_allowed_references(),
                    finder=finder,
                    client_session=session,  # so it uses the pending transaction's session
                )
                # If any of the references emanating from this document is broken,
                # we raise an HTTP 422 error and abort the transaction.
                #
                # TODO: The violation might not involve a reference to one of the
                #       target documents. The `scan_outgoing_references` function
                #       scans _all_ references emanating from the document.
                #
                # TODO: Consider (accumulating and) reporting _all_ would-be-broken references
                #       instead of only the _first_ one we encounter.
                #
                if len(violations) > 0:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"The operation was not performed, because performing it would "
                            f"have left behind one or more broken references. For example: "
                            f"The document having 'id'='{referring_document_id}' in "
                            f"the collection '{referring_collection_name}' has outgoing "
                            f"references that would be broken by the update operation. "
                            f"Update or delete referring document(s) and try again."
                        ),
                    )

            # For each updated document, check whether any of its outgoing references
            # is broken (in the context of the transaction).
            for descriptor in target_document_descriptors:
                updated_document_oid = descriptor["_id"]
                updated_document_id = descriptor["id"]
                updated_collection_name = (
                    collection_name  # makes a disambiguating alias
                )
                # Get the updated document, so we can check its outgoing references.
                # TODO: Consider projecting only necessary fields.
                updated_document = db[updated_collection_name].find_one(
                    {"_id": updated_document_oid},
                    session=session,
                )
                # If the updated document is not found, we skip it, since it means that it
                # was deleted since we identified it.
                #
                # TODO: We can prevent this from happening by performing the "identification"
                #       step within this transaction. Indeed, since we will be fetching each
                #       of the updated documents anyway, we can validate them first (before
                #       validating the original referrers), storing their `_id`s for the
                #       use when we validate the original referrers.
                #
                if updated_document is None:
                    continue
                violations = scan_outgoing_references(
                    document=updated_document,
                    source_collection_name=updated_collection_name,
                    schema_view=nmdc_schema_view(),
                    references=get_allowed_references(),
                    finder=finder,
                    client_session=session,  # so it uses the pending transaction's session
                )
                # If any of the references emanating from this document is broken,
                # we raise an HTTP 422 error and abort the transaction.
                #
                # TODO: As mentioned above, consider (accumulating and) reporting _all_
                #       would-be-broken references instead of only the _first_ one we encounter.
                #
                if len(violations) > 0:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"The operation was not performed, because performing it would "
                            f"have left behind one or more broken references. For example: "
                            f"The document having 'id'='{updated_document_id}' in "
                            f"the collection '{updated_collection_name}' has outgoing "
                            f"references that would be broken by the update operation. "
                            f"Update or delete referring document(s) and try again."
                        ),
                    )

            # Whatever happens, abort the transaction.
            #
            # Note: If an exception was raised within this `with` block, the transaction
            #       will already have been aborted automatically (and execution will not
            #       have reached this statement). On the other hand, if no exception
            #       was raised, we explicitly abort the transaction so that the updates
            #       that we "simulated" in this block do not get applied to the real database.
            #       Reference: https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html
            #
            session.abort_transaction()

    pass  # TODO: Return something (maybe instead of raising exceptions in this function).
