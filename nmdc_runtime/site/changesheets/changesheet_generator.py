# nmdc_runtime/site/changesheets/changesheet_generator.py
"""
changesheet_generator.py: Provides classes to generate and validate changesheets for NMDC database objects.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import time
from typing import ClassVar, Dict, Any, Optional

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


class ChangesheetGenerator(ABC):
    """
    Abstract base class for changesheet generators
    """
    @abstractmethod
    def __init__(self, name: str) -> None:
        pass

    @abstractmethod
    def generate_changesheet(self) -> None:
        """
        Generate a changesheet
        :return: None
        """
        pass

    @abstractmethod
    def add_changesheet_line_item(self, line_item: ChangesheetLineItem) -> None:
        """
        Add a line item to the changesheet
        :param line_item: ChangesheetLineItem
        :return: None
        """
        pass

    @abstractmethod
    def validate_changesheet(self) -> bool:
        """
        Validate a changesheet against the NMDC runtime validation API
        :return: bool
        """
        pass

    @abstractmethod
    def write_changesheet(self) -> None:
        """
        Write a changesheet to a file in .tsv format
        :return: None
        """
        pass


class BaseChangesheetGenerator(ChangesheetGenerator):
    """
    Base class for changesheet generators
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.changesheet = Changesheet()
        self.output_filename_root = f"{self.name}-{time.strftime('%Y%m%d-%H%M%S')}"

    def generate_changesheet(self) -> None:
        """
        Generate a changesheet
        :return: None
        """
        raise NotImplemented


    def add_changesheet_line_item(self, line_item: ChangesheetLineItem) -> None:
        """
        Add a line item to the changesheet
        :param line_item: ChangesheetLineItem
        :return: None
        """
        self.changesheet.line_items.append(line_item)

    def validate_changesheet(self) -> bool:
        """
        Validate a changesheet against the NMDC runtime validation API
        :return: bool
        """
        raise NotImplementedError

    def write_changesheet(self, output_filename=None) -> None:
        """
        Write a changesheet to a file in .tsv format
        :return: None
        """
        if not output_filename:
            output_filename = f"{self.output_filename_root}.tsv"

        with open(output_filename, "w") as f:
            f.write(self.changesheet.header)
            for line_item in self.changesheet.line_items:
                f.write(line_item.line)


def get_nmdc_biosample_by_id(id_: str) -> Optional[JSON_OBJECT]:
    """
    Get an NMDC biosample by ID
    :param id_: str
    :return: JSON_OBJECT
    """
    return None