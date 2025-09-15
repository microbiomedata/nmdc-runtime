import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union
from nmdc_schema import nmdc

JSON_OBJECT = Dict[str, Any]

logger = logging.getLogger(__name__)


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

    def _parse_quantity_value(
        self, raw_value: Optional[str], unit: Optional[str] = None
    ) -> Union[nmdc.QuantityValue, None]:
        """Construct a nmdc:QuantityValue from a raw value string

        The regex pattern minimally matches on a single numeric value (possibly
        floating point). The pattern can also identify a range represented by
        two numeric values separated by a hyphen. It can also identify non-numeric
        characters at the end of the string which are interpreted as a unit. A unit
        may also be explicitly provided as an argument to this function. If parsing
        identifies a unit and a unit argument is provided, the unit argument is used.
        If the pattern is not matched at all None is returned.

        :param raw_value: string to parse
        :param unit: optional unit, defaults to None. If None, the unit is extracted from the
            raw_value. If a unit is provided, it will override the unit extracted from the
            raw_value.
        :return: nmdc:QuantityValue
        """
        if raw_value is None:
            return None

        match = re.fullmatch(
            "([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*)(?:[eE][+-]?\d+)?)(?: *- *([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*)(?:[eE][+-]?\d+)?))?(?: *(\S+))?",
            raw_value,
        )
        if not match:
            return None

        quantity_value_kwargs = {
            "has_raw_value": raw_value,
            "type": "nmdc:QuantityValue",
        }
        if match.group(2):
            # having group 2 means the value is a range like "0 - 1". Either
            # group 1 or group 2 might be the minimum especially when handling
            # negative ranges like "0 - -1"
            num_1 = float(match.group(1))
            num_2 = float(match.group(2))
            quantity_value_kwargs["has_minimum_numeric_value"] = min(num_1, num_2)
            quantity_value_kwargs["has_maximum_numeric_value"] = max(num_1, num_2)
        else:
            # otherwise we just have a single numeric value
            quantity_value_kwargs["has_numeric_value"] = float(match.group(1))

        if unit:
            # a unit was manually specified
            if match.group(3) and unit != match.group(3):
                # a unit was also found in the raw string; issue a warning
                # if they don't agree, but keep the manually specified one
                logger.warning(f'Unit mismatch: "{unit}" and "{match.group(3)}"')
            quantity_value_kwargs["has_unit"] = unit
        elif match.group(3):
            # a unit was found in the raw string
            quantity_value_kwargs["has_unit"] = match.group(3)

        return nmdc.QuantityValue(**quantity_value_kwargs)
