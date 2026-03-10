"""Centralized Redis connection pool management for VobChat"""

import os
import logging
from typing import Optional
import redis
from redis.asyncio import Redis as AsyncRedis, ConnectionPool as AsyncConnectionPool
from redis import ConnectionPool as SyncConnectionPool

logger = logging.getLogger(__name__)

class RedisPoolManager:
    """Singleton manager for Redis connection pools"""
    
    _instance = None
    _sync_pool: Optional[SyncConnectionPool] = None
    _async_pool: Optional[AsyncConnectionPool] = None
    _redis_url: Optional[str] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisPoolManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            # Build Redis URL from environment variables
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = os.getenv('REDIS_PORT', '6379')
            redis_db = os.getenv('REDIS_DB', '0')
            self._redis_url = os.getenv('REDIS_URL', f'redis://{redis_host}:{redis_port}/{redis_db}')
            logger.info(f"RedisPoolManager initialized with URL: {self._redis_url}")
            self._configure_redis_persistence_once()
    
    def get_sync_pool(self) -> SyncConnectionPool:
        """Get or create synchronous Redis connection pool"""
        if self._sync_pool is None:
            self._sync_pool = SyncConnectionPool.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=50,
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30
            )
            logger.info("Created synchronous Redis connection pool")
        return self._sync_pool
    
    def get_async_pool(self, decode_responses: bool = False) -> AsyncConnectionPool:
        """Get or create asynchronous Redis connection pool"""
        if self._async_pool is None:
            self._async_pool = AsyncConnectionPool.from_url(
                self._redis_url,
                decode_responses=decode_responses,
                max_connections=50,
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30
            )
            logger.info(f"Created asynchronous Redis connection pool (decode_responses={decode_responses})")
        return self._async_pool
    
    def get_sync_client(self) -> redis.Redis:
        """Get a synchronous Redis client using the pool"""
        return redis.Redis(connection_pool=self.get_sync_pool())
    
    def get_async_client(self, decode_responses: bool = False) -> AsyncRedis:
        """Get an asynchronous Redis client using the pool"""
        return AsyncRedis(connection_pool=self.get_async_pool(decode_responses))
    
    async def close_async_pool(self):
        """Close the async connection pool gracefully"""
        if self._async_pool:
            await self._async_pool.disconnect()
            self._async_pool = None
            logger.info("Closed asynchronous Redis connection pool")
    
    def close_sync_pool(self):
        """Close the sync connection pool gracefully"""
        if self._sync_pool:
            self._sync_pool.disconnect()
            self._sync_pool = None
            logger.info("Closed synchronous Redis connection pool")
    
    def _configure_redis_persistence_once(self):
        """Configure Redis to disable RDB persistence once at startup"""
        try:
            temp_client = redis.Redis.from_url(self._redis_url)
            temp_client.config_set('save', '')
            temp_client.config_set('stop-writes-on-bgsave-error', 'no')
            temp_client.close()
            logger.info("Disabled Redis RDB persistence at startup")
        except Exception as e:
            logger.warning(f"Failed to disable Redis RDB persistence at startup: {e}")
    
    async def health_check_async(self) -> bool:
        """Check if async Redis connection is healthy"""
        try:
            client = self.get_async_client()
            await client.ping()
            return True
        except Exception as e:
            logger.error(f"Async Redis health check failed: {e}")
            return False
    
    def health_check_sync(self) -> bool:
        """Check if sync Redis connection is healthy"""
        try:
            client = self.get_sync_client()
            client.ping()
            return True
        except Exception as e:
            logger.error(f"Sync Redis health check failed: {e}")
            return False

# Global singleton instance
redis_pool_manager = RedisPoolManager()