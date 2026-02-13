"""LockManager 单元测试。"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.core.lock import MemoryLockManager, RedisLockManager

# ==================== MemoryLockManager 测试 ====================


@pytest.mark.asyncio
async def test_memory_lock_acquire_and_release():
    """基本的获取和释放锁"""
    lm = MemoryLockManager()

    assert await lm.acquire("key:1", ttl=60)
    # 重复获取同一个锁应失败
    assert not await lm.acquire("key:1", ttl=60)
    # 释放后可以重新获取
    await lm.release("key:1")
    assert await lm.acquire("key:1", ttl=60)


@pytest.mark.asyncio
async def test_memory_lock_different_keys():
    """不同的 key 互不影响"""
    lm = MemoryLockManager()

    assert await lm.acquire("key:1", ttl=60)
    assert await lm.acquire("key:2", ttl=60)


@pytest.mark.asyncio
async def test_memory_lock_expired_auto_release():
    """过期的锁应自动释放"""
    lm = MemoryLockManager()

    # 使用极短 TTL
    assert await lm.acquire("key:1", ttl=0)
    # 稍等一下让锁过期
    await asyncio.sleep(0.01)
    # 应该可以重新获取
    assert await lm.acquire("key:1", ttl=60)


@pytest.mark.asyncio
async def test_memory_lock_release_idempotent():
    """释放不存在的锁不应报错"""
    lm = MemoryLockManager()
    # 不应该抛出任何异常
    await lm.release("nonexistent")


@pytest.mark.asyncio
async def test_memory_lock_concurrent_acquire():
    """并发获取同一个锁，应只有一个成功"""
    lm = MemoryLockManager()
    results = await asyncio.gather(
        lm.acquire("key:1", ttl=60),
        lm.acquire("key:1", ttl=60),
        lm.acquire("key:1", ttl=60),
    )
    assert results.count(True) == 1
    assert results.count(False) == 2


# ==================== RedisLockManager 测试 ====================


@pytest.mark.asyncio
async def test_redis_lock_acquire_success():
    """RedisLockManager 获取锁成功"""
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)

    lm = RedisLockManager(redis)
    assert await lm.acquire("key:1", ttl=60)
    redis.set.assert_awaited_once_with("key:1", "1", ex=60, nx=True)


@pytest.mark.asyncio
async def test_redis_lock_acquire_fail():
    """RedisLockManager 获取锁失败（已被其他人持有）"""
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=False)

    lm = RedisLockManager(redis)
    assert not await lm.acquire("key:1", ttl=60)


@pytest.mark.asyncio
async def test_redis_lock_release():
    """RedisLockManager 释放锁"""
    redis = AsyncMock()
    redis.delete = AsyncMock()

    lm = RedisLockManager(redis)
    await lm.release("key:1")
    redis.delete.assert_awaited_once_with("key:1")
