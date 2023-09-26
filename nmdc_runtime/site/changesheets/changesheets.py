# nmdc_runtime/site/changesheets/changesheets.py
"""
changesheets.py: Provides classes for changesheets for NMDC database objects, and functions to generate them.
"""
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Dict, Any

JSON_OBJECT = Dict[str, Any]  # TODO: de-duplicate this with the one in translator.py
CHANGESHEETS_DIR = Path(__file__).parent.absolute().joinpath("changesheets")


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
    name: str
    header: ClassVar[str] = "id\taction\tattribute\tvalue"
    line_items: list[ChangesheetLineItem] = field(default_factory=list)

    def __post_init__(self):
        self.output_filename_root: str = f"{self.name}-{time.strftime('%Y%m%d-%H%M%S')}"

    def add_line_item(self, line_item: ChangesheetLineItem) -> None:
        """
        Add a line item to the changesheet
        :param line_item: ChangesheetLineItem
        :return: None
        """
        self.line_items.append(line_item)

    def write_changesheet(self, output_dir: Path = None) -> None:
        """
        Write a changesheet to a file in .tsv format
        :return: None
        """
        if output_dir is None:
            output_dir = CHANGESHEETS_DIR
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"{self.output_filename_root}.tsv"
        output_path = output_dir / output_filename
        with output_path.open("w") as f:
            f.write(self.header)
            for line_item in self.line_items:
                f.write(line_item.line)

    def validate_changesheet(self) -> bool:
        """
        Validate a changesheet against the NMDC runtime validation API
        :return: bool
        """
        raise NotImplementedError
