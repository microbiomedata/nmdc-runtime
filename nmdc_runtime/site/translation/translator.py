from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
from nmdc_schema import nmdc

JSON_OBJECT = Dict[str, Any]


class Translator(ABC):
    def __init__(
        self, id_minter: Optional[Callable[[str, Optional[int]], List[str]]] = None
    ) -> None:
        self._id_minter = id_minter

    def _index_by_id(self, collection, id):
        return {item[id]: item for item in collection}

    @staticmethod
    def _ensure_curie(identifier: str, *, default_prefix: str) -> str:
        identifier_parts = identifier.split(":", 1)

        # Don't add prefix if identifier is already a CURIE
        if len(identifier_parts) == 2:
            return identifier

        return f"{default_prefix}:{identifier_parts[0]}"

    @abstractmethod
    def get_database(self) -> nmdc.Database:
        pass
