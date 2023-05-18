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

    def _get_curie(self, prefix: str, local: str) -> str:
        return f"{prefix}:{local}"

    @abstractmethod
    def get_database(self) -> nmdc.Database:
        pass
