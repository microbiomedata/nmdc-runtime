import abc
import re
from typing import Union

from fastapi import HTTPException
from pymongo import ReturnDocument
from toolz import merge, dissoc
from pymongo.database import Database as MongoDatabase


from nmdc_runtime.minter.domain.model import (
    Identifier,
    Status,
    MintingRequest,
    BindingRequest,
    ResolutionRequest,
    DeleteRequest,
)
from nmdc_runtime.api.core.idgen import generate_id
from nmdc_runtime.util import find_one


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


class MongoIDStore(abc.ABC):
    def __init__(self, mdb: MongoDatabase):
        self.db = mdb

    def mint(self, req_mint: MintingRequest) -> list[Identifier]:
        if not self.db["minter.services"].find_one({"id": req_mint.service.id}):
            raise MinterError(f"Unknown service {req_mint.service.id}")
        if not self.db["minter.requesters"].find_one({"id": req_mint.requester.id}):
            raise MinterError(f"Unknown requester {req_mint.requester.id}")
        if not self.db["minter.schema_classes"].find_one(
            {"id": req_mint.schema_class.id}
        ):
            raise MinterError(f"Unknown schema class {req_mint.schema_class.id}")
        typecode = self.db["minter.typecodes"].find_one(
            {"schema_class": req_mint.schema_class.id}
        )
        if not typecode:
            raise MinterError(
                detail=f"Cannot map schema class {req_mint.schema_class.id} to a typecode"
            )
        shoulder = self.db["minter.shoulders"].find_one(
            {"assigned_to": req_mint.service.id}
        )
        collected = []
        while True:
            id_names = set()
            n_to_generate = req_mint.how_many - len(collected)
            while len(id_names) < n_to_generate:
                blade = generate_id(length=8, split_every=0, checksum=True)
                id_name = f"nmdc:{typecode['name']}-{shoulder['name']}-{blade}"
                id_names.add(id_name)
            id_names = list(id_names)
            taken = {
                d["id"]
                for d in self.db["minter.id_records"].find(
                    {"id": {"$in": id_names}}, {"id": 1}
                )
            }
            not_taken = [n for n in id_names if n not in taken]
            if not_taken:
                ids = [
                    Identifier(
                        **{
                            "id": id_name,
                            "name": id_name,
                            "typecode": typecode,
                            "shoulder": shoulder,
                            "status": Status.draft,
                        }
                    )
                    for id_name in not_taken
                ]
                self.db["minter.id_records"].insert_many([i.model_dump() for i in ids])
                collected.extend(ids)
            if len(collected) == req_mint.how_many:
                break
        return collected

    def bind(self, req_bind: BindingRequest) -> Identifier:
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
        match re.match(r"nmdc:([^-]+)-([^-]+)-.*", req_res.id_name).groups():
            case (_, _):
                doc = self.db["minter.id_records"].find_one({"id": req_res.id_name})
                # TODO if draft ID, check requester
                return Identifier(**doc) if doc else None
            case _:
                raise MinterError("Invalid ID name")

    def delete(self, req_del: DeleteRequest):
        id_stored = self.resolve(req_del)
        if id_stored is None:
            raise MinterError(f"ID {req_del.id_name} is unknown")

        match id_stored.status:
            case Status.draft:
                self.db["minter.id_records"].delete_one({"id": id_stored.id})
            case _:
                raise MinterError("Status not 'draft'. Can't delete.")
