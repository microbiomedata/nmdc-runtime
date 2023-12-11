# nmdc_runtime/site/changesheets/base.py
"""
base.py: Provides data classes for creating changesheets for NMDC database objects.
"""

import logging
import os
import time
from dataclasses import dataclass, field
from dotenv import load_dotenv
from pathlib import Path
import requests
from typing import Any, ClassVar, Dict, TypeAlias, Optional

from nmdc_runtime.site.resources import GoldApiClient, RuntimeApiUserClient

load_dotenv()
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
        """
        Return the line item as a tab-separated string
        """
        cleaned_value = self.value.replace("\n", " ").replace("\t", " ").strip()
        return f"{self.id}\t{self.action}\t{self.attribute}\t{cleaned_value}"


@dataclass
class Changesheet:
    """
    A changesheet
    """

    name: str
    line_items: list = field(default_factory=list)
    header: ClassVar[str] = "id\taction\tattribute\tvalue"
    output_dir: Optional[Path] = None

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


def get_runtime_client(use_dev_api):
    if use_dev_api:
        base_url = os.getenv("API_HOST_DEV")
        logging.info("using Dev API...")
    else:
        base_url = os.getenv("API_HOST")
        logging.info("using prod API...")
    return RuntimeApiUserClient(
        base_url=base_url,
        username=os.getenv("API_QUERY_USER"),
        password=os.getenv("API_QUERY_PASS"),
    )


def get_gold_client():
    return GoldApiClient(
        base_url=os.getenv("GOLD_API_BASE_URL"),
        username=os.getenv("GOLD_API_USERNAME"),
        password=os.getenv("GOLD_API_PASSWORD"),
    )
