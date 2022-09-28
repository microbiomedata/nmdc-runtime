"""
Database initialization
"""
import os

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

# from nmdc_runtime.infrastructure.database.impl.mongo.models import User
from components.workflow.workflow.core import get_beanie_documents


async def mongo_beanie_init(app):
    """Initialize database service"""
    document_models = get_beanie_documents()
    app.db = AsyncIOMotorClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
    )[os.getenv("MONGO_DBNAME")]
    await init_beanie(app.db, document_models=document_models)
