# nmdc_runtime/site/changesheets/changesheets.py
"""
changesheets.py: Provides classes for changesheets for NMDC database objects.
"""
from dataclasses import dataclass, field
from typing import ClassVar, Optional, Dict, Any



JSON_OBJECT = Dict[str, Any]  # TODO: de-duplicate this with the one in translator.py

@dataclass
class ChangesheetLineItem:
    """ Dataclass representing a line-item in a changesheet"""
    id: str
    action: str
    attribute: str
    value: str

    @property
    def line(self) -> str:
        return f"{self.id}\t{self.action}\t{self.attribute}\t{self.value}"


@dataclass
class Changesheet:
    """ Dataclass representing a changesheet being generated """
    header: ClassVar[str] = "id\taction\tattribute\tvalue"
    line_items: list[ChangesheetLineItem] = field(default_factory=list)





def get_nmdc_biosample_by_id(id_: str) -> Optional[JSON_OBJECT]:
    """
    Get an NMDC biosample by ID
    :param id_: str
    :return: JSON_OBJECT
    """
    return None
