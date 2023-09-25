# nmdc_runtime/site/changesheets/changesheets.py
"""
changesheets.py: Provides classes for changesheets for NMDC database objects, and functions to generate them.
"""
from dagster import op, graph, resource, AssetMaterialization
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Optional, Dict, Any



JSON_OBJECT = Dict[str, Any]  # TODO: de-duplicate this with the one in translator.py

config_test = {
    "resources": {
        "mongo": {
            "config": {
                # local docker container via docker-compose.yml
                "host": "mongo",
                "username": "admin",
                "password": "root",
                "dbname": "nmdc_etl_staging",
            },
        }
    },
    "ops": {
        "load_nmdc_etl_class": {
            "config": {
                "data_file": str(
                    Path(__file__).parent.parent.parent.parent.joinpath(
                        "metadata-translation/src/data/nmdc_merged_data.tsv.zip"
                    )
                ),
                "sssom_map_file": "",
                "spec_file": str(
                    Path(__file__).parent.parent.parent.parent.joinpath(
                        "nmdc_runtime/lib/nmdc_data_source.yaml"
                    )
                ),
            }
        }
    },
}

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
