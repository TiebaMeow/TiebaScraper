"""分布式锁管理器模块。

提供 Redis 和内存两种锁实现，通过统一的协议接口供 TaskHandler 使用。
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol


class LockManager(Protocol):
    """锁管理器协议。"""

    async def acquire(self, key: str, ttl: int = 300) -> bool:
        """尝试获取锁。

        Args:
            key: 锁的唯一标识键。
            ttl: 锁的过期时间（秒），默认 300 秒。

        Returns:
            是否成功获取锁。
        """
        ...

    async def release(self, key: str) -> None:
        """释放锁（幂等）。

        Args:
            key: 锁的唯一标识键。
        """
        ...


class RedisLockManager:
    """基于 Redis ``SET NX`` 的分布式锁管理器。"""

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    async def acquire(self, key: str, ttl: int = 300) -> bool:
        return await self._redis.set(key, "1", ex=ttl, nx=True)

    async def release(self, key: str) -> None:
        await self._redis.delete(key)


class MemoryLockManager:
    """基于内存字典的锁管理器（单进程降级方案）。"""

    def __init__(self) -> None:
        self._locks: dict[str, float] = {}
        self._guard = asyncio.Lock()

    async def acquire(self, key: str, ttl: int = 300) -> bool:
        async with self._guard:
            now = time.time()
            # 清理过期锁，保留未过期的锁
            self._locks = {k: t for k, t in self._locks.items() if now <= t}

            if key in self._locks:
                return False

            self._locks[key] = now + ttl
            return True

    async def release(self, key: str) -> None:
        async with self._guard:
            self._locks.pop(key, None)
