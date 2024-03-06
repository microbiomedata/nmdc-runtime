"""
Database initialization
"""

import os

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from nmdc_runtime.infrastructure.database.impl.mongo.models import User

# async def mongo_init(app):
#     """Initialize database service"""
#     app.db = AsyncIOMotorClient(
#         host=os.getenv("MONGO_HOST"),
#         username=os.getenv("MONGO_USERNAME"),
#         password=os.getenv("MONGO_DBNAME"),
#     ).account
#     await init_beanie(app.db, document_models=[User])
