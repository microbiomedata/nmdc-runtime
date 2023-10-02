# nmdc_runtime/site/changesheets/base.py
"""
base.py: Provides data classes for creating changesheets for NMDC database objects.
"""

from dataclasses import dataclass
import time
from typing import Dict, Any


JSON_OBJECT = Dict[str, Any]
@dataclass
class ChangesheetLineItem:
    """
    A line item in a changesheet
    """
    id: str
    action: str
    attribute: str
    value: str


@dataclass
class Changesheet:
    """
    A changesheet
    """
    name: str
    line_items: list = None

    def __post_init__(self):
        self.line_items = []
        self.output_filename_root: str = f"{self.name}-{time.strftime('%Y%m%d-%H%M%S')}"

    def validate_changesheet(self):
        """
        Validate the changesheet
        :return: None
        """
        raise NotImplementedError

    def write_changesheet(self):
        """
        Write the changesheet to a file
        :return: None
        """
        raise NotImplementedError