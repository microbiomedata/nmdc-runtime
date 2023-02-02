import json
from dataclasses import asdict, fields
from datetime import datetime
from typing import Dict, List, Literal, TypedDict, Union, cast

import attrs
from nmdc_schema.nmdc import Database
from pymongo.database import Database as MongoDatabase


def insert_activities(activities: Database, mdb: MongoDatabase) -> bool:
    """Description."""
    activity_fields = fields(activities)
    for field in activity_fields:
        if activities[field.name]:
            collection = mdb.get_collection(field.name)
            collection.insert_many(
                [
                    json.loads(json.dumps(asdict(activity), default=str))
                    for activity in activities[field.name]
                ]
            )
    return True
