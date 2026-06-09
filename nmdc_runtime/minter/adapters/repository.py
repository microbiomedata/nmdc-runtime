import abc
import logging
import re
from typing import Union

from pymongo import ReturnDocument
from toolz import merge
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.endpoints.lib.workflow_executions import (
    parse_workflow_execution_id,
    make_pattern_matching_ids_having_base_id,
)
from nmdc_runtime.minter.domain.model import (
    Identifier,
    Status,
    MintingRequest,
    BindingRequest,
    ResolutionRequest,
    DeleteRequest,
    WorkflowExecutionIdMintingRequest,
)
from nmdc_runtime.api.core.idgen import generate_id
from nmdc_runtime.util import find_one

logger = logging.getLogger(__name__)


class MinterError(Exception):
    def __init__(
        self,
        detail: str,
    ) -> None:
        self.detail = detail

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}(detail={self.detail!r})"


class IDStore(abc.ABC):
    @abc.abstractmethod
    def mint(self, req_mint: MintingRequest) -> list[Identifier]:
        raise NotImplementedError

    @abc.abstractmethod
    def bind(self, req_bind: BindingRequest) -> Identifier:
        raise NotImplementedError

    @abc.abstractmethod
    def resolve(self, req_res: ResolutionRequest) -> Union[Identifier, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, req_del: DeleteRequest):
        raise NotImplementedError


class InMemoryIDStore(IDStore):
    def __init__(
        self,
        services=None,
        shoulders=None,
        typecodes=None,
        requesters=None,
        schema_classes=None,
    ):
        self.db = {}
        self.services = services
        self.shoulders = shoulders
        self.typecodes = typecodes
        self.requesters = requesters
        self.schema_classes = schema_classes

    def mint(self, req_mint: MintingRequest) -> list[Identifier]:
        if not find_one({"id": req_mint.service.id}, self.services):
            raise MinterError("Unknown service {req_mint.service.id}")
        if not find_one({"id": req_mint.requester.id}, self.requesters):
            raise MinterError(f"Unknown requester {req_mint.requester.id}")
        if not find_one({"id": req_mint.schema_class.id}, self.schema_classes):
            raise MinterError(f"Unknown schema class {req_mint.schema_class.id}")
        # ensure supplied schema class has typecode
        typecode = find_one({"schema_class": req_mint.schema_class.id}, self.typecodes)
        if not typecode:
            raise MinterError(
                detail=f"Cannot map schema class {req_mint.schema_class.id} to a typecode"
            )

        ids = []
        for _ in range(req_mint.how_many):
            blade = generate_id(length=8, split_every=0)
            shoulder = find_one({"assigned_to": req_mint.service.id}, self.shoulders)
            id_name = f"nmdc:{typecode['name']}-{shoulder['name']}-{blade}"
            ids.append(
                Identifier(
                    **{
                        "id": id_name,
                        "name": id_name,
                        "typecode": typecode,
                        "shoulder": shoulder,
                        "status": Status.draft,
                    }
                )
            )
        for id_ in ids:
            self.db[id_.id] = id_.model_dump()
        return ids

    def bind(self, req_bind: BindingRequest) -> Identifier:
        id_stored = self.resolve(req_bind)
        if id_stored is None:
            raise MinterError(f"ID {req_bind.id_name} is unknown")

        match id_stored.status:
            case Status.draft:
                self.db[id_stored.id] = merge(
                    self.db[id_stored.id], {"bindings": req_bind.metadata_record}
                )
                return Identifier(**self.db[id_stored.id])
            case _:
                raise MinterError(
                    detail="Status not 'draft'. Can't change bound metadata"
                )

    def resolve(self, req_res: ResolutionRequest) -> Union[Identifier, None]:
        doc = self.db.get(req_res.id_name)
        return Identifier(**doc) if doc else None

    def delete(self, req_del: DeleteRequest):
        id_stored = self.resolve(req_del)
        if id_stored is None:
            raise MinterError(f"ID {req_del.id_name} is unknown")

        match id_stored.status:
            case Status.draft:
                self.db.pop(id_stored.id)
            case _:
                raise MinterError("Status not 'draft'. Can't delete.")


class MongoIDStore(IDStore):
    def __init__(self, mdb: MongoDatabase):
        self.db = mdb

    def _validate_service_id(self, service_id: str | None) -> None:
        """Raises an exception if the specified service ID is not found."""
        if not self.db["minter.services"].find_one({"id": service_id}):
            raise MinterError(f"Unknown service {service_id}")

    def _validate_requester_id(self, requester_id: str | None) -> None:
        """Raises an exception if the specified requester ID is not found."""
        if not self.db["minter.requesters"].find_one({"id": requester_id}):
            raise MinterError(f"Unknown requester {requester_id}")

    def _validate_schema_class_uri(self, schema_class_uri: str | None) -> None:
        """Raises an exception if the specified schema class URI is not found."""
        if not self.db["minter.schema_classes"].find_one({"id": schema_class_uri}):
            raise MinterError(f"Unknown schema class {schema_class_uri}")

    def _validate_minting_request(self, minting_request: MintingRequest) -> None:
        """
        Raises an exception if the specified minting request fails validation.

        Note: This validation logic was copied verbatim from the original `mint` method.
              It may not be comprehensive.
        """
        self._validate_service_id(minting_request.service.id)
        self._validate_requester_id(minting_request.requester.id)
        self._validate_schema_class_uri(minting_request.schema_class.id)

    def _get_typecode_document_by_class_uri(self, class_uri: str | None):
        """
        Returns the Mongo document representing the typecode associated with the specified class URI,
        raising an exception if there is no such typecode.
        """
        typecode_document = self.db["minter.typecodes"].find_one(
            {"schema_class": class_uri}
        )
        if typecode_document is None:
            raise MinterError(f"Cannot map schema class {class_uri} to a typecode")
        return typecode_document

    def _get_shoulder_document_by_service_id(self, service_id: str | None):
        """
        Returns the Mongo document representing the shoulder assigned to the service having the specified ID,
        raising an exception if there is no such shoulder.
        """
        shoulder_document = self.db["minter.shoulders"].find_one(
            {"assigned_to": service_id}
        )
        if shoulder_document is None:
            raise MinterError(f"Failed to find shoulder assigned to service.")
        return shoulder_document

    def _check_id_availability(self, ids: list[str]) -> tuple[list[str], list[str]]:
        """
        Returns a tuple whose first item is a list of the specified IDs that are available,
        and whose second item is a list of the specified IDs that are taken (not available).
        """

        cursor = self.db["minter.id_records"].find({"id": {"$in": ids}}, {"id": 1})
        ids_taken = [document["id"] for document in cursor]
        ids_available = [id_ for id_ in ids if id_ not in ids_taken]
        return ids_available, ids_taken

    def mint(self, req_mint: MintingRequest) -> list[Identifier]:
        """
        Mint the specified number of `id` values, for the schema class having the specified class URI.
        """

        self._validate_minting_request(minting_request=req_mint)
        typecode_document = self._get_typecode_document_by_class_uri(
            class_uri=req_mint.schema_class.id
        )
        shoulder_document = self._get_shoulder_document_by_service_id(
            service_id=req_mint.service.id
        )
        typecode_str = typecode_document["name"]  # e.g. "sty" or "bsm"
        shoulder_str = shoulder_document["name"]  # e.g. "11"

        # Mint the requested quantity of `id`s.
        ids_minted = []
        while True:
            # Note: I assume the author opted to not call this `ids` because that term would be "overloaded"
            #       in this context. On the other hand, `id` documents _do_ have a `name` field (see the
            #       parameters being passed to `Identifier` further down in this function).
            id_names = set()
            num_ids_to_generate = req_mint.how_many - len(ids_minted)
            while len(id_names) < num_ids_to_generate:
                blade = generate_id(length=8, split_every=0, checksum=True)
                id_name = f"nmdc:{typecode_str}-{shoulder_str}-{blade}"
                id_names.add(id_name)
            id_names = list(id_names)
            ids_not_taken, _ = self._check_id_availability(ids=id_names)

            # If any of the generated `id` values was not taken, take it now.
            #
            # TODO: There's a race condition here, since an `id` might be taken between when we checked and when
            #       we take it. If that happens, the `insert_many` below will fail since the `minter.id_records`
            #       MongoDB collection has a unique index on its `id` field.
            #
            if len(ids_not_taken) > 0:
                identifiers = [
                    Identifier(
                        **{
                            "id": id_name,
                            "name": id_name,
                            "typecode": typecode_document,
                            "shoulder": shoulder_document,
                            "status": Status.draft,
                        }
                    )
                    for id_name in ids_not_taken
                ]
                self.db["minter.id_records"].insert_many(
                    [i.model_dump() for i in identifiers]
                )
                ids_minted.extend(identifiers)

            # Once we've minted the same number of `id`s as were requested, break out of the forever loop.
            if len(ids_minted) == req_mint.how_many:
                break
        return ids_minted

    def mint_workflow_execution_id(
        self,
        minting_request: WorkflowExecutionIdMintingRequest,
    ) -> Identifier:
        """
        Mint an `id` value for the specified `WorkflowExecution` subclass. If an existing `id`
        is specified, the minted `id` will have the same base as that `id`.
        """

        # Validate the minting request and extract the typecode and shoulder from it.
        self._validate_service_id(minting_request.service.id)
        self._validate_requester_id(minting_request.requester.id)
        self._validate_schema_class_uri(minting_request.schema_class.id)
        typecode_document = self._get_typecode_document_by_class_uri(
            class_uri=minting_request.schema_class.id
        )
        shoulder_document = self._get_shoulder_document_by_service_id(
            service_id=minting_request.service.id
        )

        # If no base ID was specified, mint one now. This ensures we have a base ID for the subsequent steps.
        base_id = minting_request.base_id
        if base_id is None:
            identifiers: list[Identifier] = self.mint(
                MintingRequest(
                    service=minting_request.service,
                    requester=minting_request.requester,
                    schema_class=minting_request.schema_class,
                    how_many=1,
                )
            )
            base_id = identifiers[0].name
            logger.info(f"Minted base ID: {base_id}")

        # Do both the "find" and "claim" steps within a single MongoDB transaction to avoid race conditions.
        with self.db.client.start_session() as client_session:
            with client_session.start_transaction():
                minter_id_records = self.db.get_collection(
                    "minter.id_records"
                )  # concise alias
                workflow_execution_set = self.db.get_collection(
                    "workflow_execution_set"
                )  # concise alias

                # Gather all the integers from the already-claimed "dot integer" suffixes for the given base ID.
                #
                # Important: Due to {{{ ...see note in `fastapi_app.py` about the history of "ID management"... }}},
                #            we check _both_ the `minter.id_records` collection and the `workflow_execution_set`
                #            collection.
                #
                integers_claimed = set()
                id_pattern = make_pattern_matching_ids_having_base_id(base_id=base_id)
                filter_ = {"id": {"$regex": id_pattern}}
                cursor = minter_id_records.find(
                    filter_, {"id": 1}, session=client_session
                )
                for document in cursor:
                    _, integer_claimed = parse_workflow_execution_id(
                        raw_id=document["id"]
                    )
                    if isinstance(integer_claimed, int):
                        integers_claimed.add(integer_claimed)
                cursor = workflow_execution_set.find(
                    filter_, {"id": 1}, session=client_session
                )
                for document in cursor:
                    _, integer_claimed = parse_workflow_execution_id(
                        raw_id=document["id"]
                    )
                    if isinstance(integer_claimed, int):
                        integers_claimed.add(integer_claimed)
                logger.info(f"Suffix integers already claimed: {integers_claimed}")

                # Calculate the integer we will claim.
                # Example: if ".1" and ".3" are claimed, we will claim ".4" (not ".2").
                largest_integer_claimed = (
                    max(integers_claimed) if len(integers_claimed) > 0 else 0
                )
                integer_to_claim = largest_integer_claimed + 1
                logger.info(f"Suffix integer to claim: {integer_to_claim}")

                # Mint an ID that has the specified base ID and that calculated integer in its "dot integer" suffix.
                id_name = f"{base_id}.{integer_to_claim}"
                identifier = Identifier(
                    **{
                        "id": id_name,
                        "name": id_name,
                        "typecode": typecode_document,
                        "shoulder": shoulder_document,
                        "status": Status.draft,
                    }
                )
                minter_id_records.insert_one(
                    identifier.model_dump(), session=client_session
                )
                logger.info(f"Minted WorkflowExecution ID: {id_name}")

        return identifier

    def bind(self, req_bind: BindingRequest) -> Identifier:
        """Associate the specified arbitrary metadata with the specified ID.

        TODO: Do not allow users to bind identifiers minted by _other_ users.
        """
        id_stored = self.resolve(req_bind)
        if id_stored is None:
            raise MinterError(f"ID {req_bind.id_name} is unknown")

        match id_stored.status:
            case Status.draft:
                return self.db["minter.id_records"].find_one_and_update(
                    {"id": id_stored.id},
                    {"$set": {"bindings": req_bind.metadata_record}},
                    return_document=ReturnDocument.AFTER,
                )
            case _:
                raise MinterError(
                    detail="Status not 'draft'. Can't change bound metadata"
                )

    def resolve(self, req_res: ResolutionRequest) -> Union[Identifier, None]:
        """Get the metadata that is bound to the specified identifier."""
        match re.match(r"nmdc:([^-]+)-([^-]+)-.*", req_res.id_name).groups():
            case (_, _):
                doc = self.db["minter.id_records"].find_one({"id": req_res.id_name})
                # TODO if draft ID, check requester
                #
                #      Note: The above "TODO" comment is about checking whether the user that wants to
                #            resolve the identifier, is the same user that minted the identifier. If
                #            it isn't, then... what? (i.e. allow resolution, or deny resolution)?
                #
                return Identifier(**doc) if doc else None
            case _:
                raise MinterError("Invalid ID name")

    def delete(self, req_del: DeleteRequest):
        """Delete an identifier that is still in the draft state.

        Note: You can mint (draft) as many IDs as you want. As long as you don't bind them
              (i.e. as long as they are still in the draft state), you can still delete them.

        TODO: Do not allow users to delete identifiers minted by _other_ users.
        """
        id_stored = self.resolve(req_del)
        if id_stored is None:
            raise MinterError(f"ID {req_del.id_name} is unknown")

        match id_stored.status:
            case Status.draft:
                self.db["minter.id_records"].delete_one({"id": id_stored.id})
            case _:
                raise MinterError("Status not 'draft'. Can't delete.")
