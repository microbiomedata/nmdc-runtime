from nmdc_runtime.db.database_manager import DatabaseManager
from nmdc_runtime.db.impl.mongo import MongoManager

db = MongoManager()


async def get_database() -> DatabaseManager:
    return db
