# nmdc_runtime/site/changesheets/base.py
"""
base.py: Provides data classes for creating changesheets for NMDC database objects.
"""

from dataclasses import dataclass
from pathlib import Path
import time
from typing import Dict, Any, ClassVar


JSON_OBJECT = Dict[str, Any]
CHANGESHEETS_DIR = Path(__file__).parent.absolute().joinpath("changesheets_output")


@dataclass
class ChangesheetLineItem:
    """
    A line item in a changesheet
    """

    id: str
    action: str
    attribute: str
    value: str

    @property
    def line(self) -> str:
        return f"{self.id}\t{self.action}\t{self.attribute}\t{self.value}"


@dataclass
class Changesheet:
    """
    A changesheet
    """

    name: str
    line_items: list = None
    header: ClassVar[str] = "id\taction\tattribute\tvalue"

    def __post_init__(self):
        self.line_items = []
        self.output_filename_root: str = f"{self.name}-{time.strftime('%Y%m%d-%H%M%S')}"

    def validate_changesheet(self):
        """
        Validate the changesheet
        :return: None
        """
        raise NotImplementedError

    def write_changesheet(self, output_dir: Path = None) -> None:
        """
        Write the changesheet to a file
        :return: None
        """
        if output_dir is None:
            output_dir = CHANGESHEETS_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"{self.output_filename_root}.tsv"
        output_filepath = output_dir.joinpath(output_filename)
        with open(output_filepath, "w") as f:
            f.write(self.header + "\n")
            for line_item in self.line_items:
                f.write(line_item.line + "\n")
