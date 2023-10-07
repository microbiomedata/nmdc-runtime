# nmdc_runtime/site/changesheets/base.py
"""
base.py: Provides data classes for creating changesheets for NMDC database objects.
"""

import logging
import time
from dataclasses import dataclass
from pathlib import Path
import requests
from typing import Any, ClassVar, Dict, TypeAlias

from nmdc_runtime.site.resources import RuntimeApiUserClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(" "message)s"
)

JSON_OBJECT: TypeAlias = Dict[str, Any]
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
    output_dir: Path = None

    def __post_init__(self):
        self.line_items = []
        if self.output_dir is None:
            self.output_dir = CHANGESHEETS_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_filename_root: str = f"{self.name}-{time.strftime('%Y%m%d-%H%M%S')}"
        self.output_filename: str = f"{self.output_filename_root}.tsv"
        self.output_filepath: Path = self.output_dir.joinpath(self.output_filename)

    def validate_changesheet(self, base_url: str) -> bool:
        """
        Validate the changesheet
        :return: None
        """
        logging.info(f"Validating changesheet {self.output_filepath}")
        url = f"{base_url}/metadata/changesheets:validate"
        resp = requests.post(
            url,
            files={"uploaded_file": open(self.output_filepath, "rb")},
        )
        return resp.ok

    def write_changesheet(self) -> None:
        """
        Write the changesheet to a file
        :return: None
        """
        with open(self.output_filepath, "w") as f:
            logging.info(f"Writing changesheet to {self.output_filepath}")
            f.write(self.header + "\n")
            for line_item in self.line_items:
                f.write(line_item.line + "\n")
