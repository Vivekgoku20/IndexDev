import json
from datetime import date
from typing import Any, Optional
import redis.asyncio as redis
from redis.exceptions import RedisError

class RedisCache:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._connected = False

    async def ensure_connected(self) -> bool:
        """Check if Redis is available"""
        if self._connected:
            return True
        try:
            await self.redis.ping()
            self._connected = True
            return True
        except RedisError:
            self._connected = False
            return False

    def build_single_date_key(self, prefix: str, date_value: date) -> str:
        """Build a cache key for a single date"""
        return f"{prefix}:{date_value.isoformat()}"

    def build_key(self, prefix: str, start_date: date, end_date: date) -> str:
        """Build a cache key for a date range"""
        return f"{prefix}:{start_date.isoformat()}:{end_date.isoformat()}"

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache, returns None if Redis is unavailable"""
        try:
            if not await self.ensure_connected():
                return None
            value = await self.redis.get(key)
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    # If it's not JSON, return the string value
                    return value
            return None
        except RedisError as e:
            print(f"Redis error during get: {e}")
            return None

    async def set(self, key: str, value: Any, expire: int = 86400) -> bool:
        """Set value in cache with expiration (default 1 day), returns False if Redis is unavailable"""
        try:
            if not await self.ensure_connected():
                return False
            # Convert value to string if needed
            if not isinstance(value, str):
                value = json.dumps(value)
            await self.redis.set(key, value, ex=expire)
            return True
        except RedisError as e:
            print(f"Redis error during set: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete value from cache, returns False if Redis is unavailable"""
        try:
            if not await self.ensure_connected():
                return False
            if isinstance(key, str):
                key = key.encode('utf-8')
            await self.redis.delete(key)
            return True
        except RedisError as e:
            print(f"Redis error during delete: {e}")
            return False
