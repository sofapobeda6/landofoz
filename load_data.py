import asyncio
import asyncpg
import json
import os
from datetime import datetime
import logging
import sys
import time
from typing import List, Tuple, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres'),
            'database': os.getenv('DB_NAME', 'video_stats')
        }
        self.conn: Optional[asyncpg.Connection] = None
        self.batch_sizes = {
            'videos': 100,
            'snapshots': 500
        }
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def connect(self) -> None:
        try:
            self.conn = await asyncpg.connect(**self.db_config)
            logger.info(f"Connected to database")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
    
    async def wait_for_postgres(self, max_attempts: int = 30) -> bool:
        logger.info("Waiting for PostgreSQL...")
        for attempt in range(1, max_attempts + 1):
            try:
                conn = await asyncpg.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database='postgres',
                    timeout=2
                )
                await conn.close()
                return True
            except Exception:
                await asyncio.sleep(2)
        logger.error(f"PostgreSQL not ready after {max_attempts} attempts")
        return False
    
    async def ensure_database_exists(self) -> bool:
        try:
            conn = await asyncpg.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database='postgres'
            )
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                self.db_config['database']
            )
            await conn.close()
            if not exists:
                logger.error(f"Database {self.db_config['database']} does not exist")
                return False
            return True  
        except Exception as e:
            logger.error(f"Error checking database: {e}")
            return False
    
    async def get_record_count(self, table: str) -> int:
        try:
            return await self.conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        except Exception:
            return 0
    
    async def needs_data_load(self) -> bool:
        video_count = await self.get_record_count('videos')
        if video_count > 0:
            logger.info(f"Data already exists ({video_count} videos)")
            return False
        else:
            logger.info("Database is empty - loading data")
            return True
    
    def find_json_file(self) -> Optional[str]:
        search_paths = [
            '/app/data/videos.json',
            'data/videos.json',
            'videos.json'
        ]
        for path in search_paths:
            if os.path.exists(path):
                logger.info(f"Found data file: {path}")
                return path
        logger.warning("videos.json not found")
        return None
    
    def parse_datetime(self, dt_str: str) -> datetime:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    
    def prepare_video_batch(self, videos: List[dict]) -> List[Tuple]:
        batch = []
        for video in videos:
            batch.append((
                video['id'],
                video['creator_id'],
                self.parse_datetime(video['video_created_at']),
                video['views_count'],
                video['likes_count'],
                video['comments_count'],
                video['reports_count'],
                self.parse_datetime(video['created_at']),
                self.parse_datetime(video['updated_at'])
            ))
        return batch
    
    async def insert_batch(self, data: List[Tuple], query: str) -> None:
        if not data:
            return
        async with self.conn.transaction():
            await self.conn.executemany(query, data)
    
    async def load_videos(self, videos: List[dict]) -> int:
        total = len(videos)
        if total == 0:
            return 0
        
        logger.info(f"Loading {total} videos...")
        query = """
            INSERT INTO videos (
                id, creator_id, video_created_at, 
                views_count, likes_count, comments_count, reports_count,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING
        """
        batch_size = self.batch_sizes['videos']
        loaded = 0
        
        for i in range(0, total, batch_size):
            batch_videos = videos[i:i + batch_size]
            batch_data = self.prepare_video_batch(batch_videos)
            await self.insert_batch(batch_data, query)
            loaded += len(batch_videos)
        
        logger.info(f"Videos loaded: {loaded}")
        return loaded
    
    async def load_snapshots(self, videos: List[dict]) -> int:
        total = sum(len(v.get('snapshots', [])) for v in videos)
        if total == 0:
            return 0
        
        logger.info(f"Loading {total} snapshots...")
        query = """
            INSERT INTO video_snapshots (
                id, video_id, 
                views_count, likes_count, comments_count, reports_count,
                delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO NOTHING
        """
        
        batch_size = self.batch_sizes['snapshots']
        loaded = 0
        batch_data = []
        for video in videos:
            video_id = video['id']
            snapshots = video.get('snapshots', [])
            for snap in snapshots:
                batch_data.append((
                    snap['id'],
                    video_id,
                    snap['views_count'],
                    snap['likes_count'],
                    snap['comments_count'],
                    snap['reports_count'],
                    snap['delta_views_count'],
                    snap['delta_likes_count'],
                    snap['delta_comments_count'],
                    snap['delta_reports_count'],
                    self.parse_datetime(snap['created_at']),
                    self.parse_datetime(snap['updated_at'])
                ))
                if len(batch_data) >= batch_size:
                    await self.insert_batch(batch_data, query)
                    loaded += len(batch_data)
                    batch_data = []
        if batch_data:
            await self.insert_batch(batch_data, query)
            loaded += len(batch_data)
        logger.info(f"Snapshots loaded: {loaded}")
        return loaded
    
    async def load_json_data(self, json_path: str) -> Tuple[int, int]:
        logger.info(f"Reading data from {json_path}")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        videos = data.get('videos', [])
        logger.info(f"Found {len(videos)} videos")

        video_count = await self.load_videos(videos)
        snapshot_count = await self.load_snapshots(videos)
        return video_count, snapshot_count
    
    async def initialize(self) -> bool:
        if not await self.wait_for_postgres():
            return False
        if not await self.ensure_database_exists():
            return False
        
        await self.connect()
        try:
            if not await self.needs_data_load():
                return True
            json_path = self.find_json_file()
            if not json_path:
                logger.warning("Skipping data load")
                return True
            video_count, snapshot_count = await self.load_json_data(json_path)
            logger.info(f"Load completed: {video_count} videos, {snapshot_count} snapshots")
            return True
        finally:
            await self.close()

async def main():
    start_time = time.time()
    try:
        if __name__ == "__main__":
            async with DataLoader() as loader:
                success = await loader.initialize()
                if not success:
                    logger.error("Data loading failed")
                    sys.exit(1)
                elapsed = time.time() - start_time
                logger.info(f"Total time: {elapsed:.2f} seconds")      
    except KeyboardInterrupt:
        logger.info("Loading interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
else:
    data_loader = DataLoader()