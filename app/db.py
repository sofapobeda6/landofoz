import asyncpg
import logging
from .config import config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool = None
        self.connected = False

    async def connect(self):
        try:
            logger.info("Connecting to PostgreSQL...")
            self.pool = await asyncpg.create_pool(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            self.connected = True
            logger.info("Database connected successfully")
            async with self.pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                logger.info(f"PostgreSQL version: {version[:50]}...")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            self.connected = False
            raise
    
    async def close(self):
        if self.pool:
            await self.pool.close()
            self.connected = False
            logger.info("Database connection closed")
    
    async def execute_query(self, query: str) -> int:
        if not self.pool or not self.connected:
            logger.warning("No database connection, reconnecting...")
            await self.connect()
        try:
            async with self.pool.acquire() as conn:
                logger.debug(f"Executing query: {query}")
                result = await conn.fetchval(query)
                logger.debug(f"Query result: {result}")
                return result if result is not None else 0
        except Exception as e:
            logger.error(f"Query error: {e}")
            logger.error(f"Query: {query}")
            self.connected = False
            raise

db = Database()