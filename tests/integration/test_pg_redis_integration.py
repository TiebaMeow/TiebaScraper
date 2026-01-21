"""PostgreSQL 和 Redis 集成测试。

这些测试只在 CI 环境中运行，需要真实的 PostgreSQL 和 Redis 服务。
"""

import json
import os
from datetime import UTC, datetime
from typing import cast

import pytest
import redis.asyncio as redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from tiebameow.models.orm import Base, Forum

from src.core.config import ConsumerConfig
from src.core.publisher import EventEnvelope, RedisStreamsPublisher

ON_CI = os.getenv("CI", "").lower() == "true" or os.getenv("GITHUB_ACTIONS") == "true"


def _get_pg_dsn() -> str:
    """获取 PostgreSQL 连接字符串"""
    pg_user = os.getenv("PGUSER") or os.getenv("DB_USER", "postgres")
    pg_password = os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD", "postgres")
    pg_host = os.getenv("PGHOST") or os.getenv("DB_HOST", "127.0.0.1")
    pg_port = int(os.getenv("PGPORT") or os.getenv("DB_PORT", "5432"))
    pg_db = os.getenv("PGDATABASE") or os.getenv("DB_NAME", "tiebascraper")
    return f"postgresql+asyncpg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"


def _get_redis_url() -> str:
    """获取 Redis 连接字符串"""
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    return f"redis://{host}:{port}/0"


@pytest.mark.skipif(not ON_CI, reason="Integration test only runs on CI")
@pytest.mark.asyncio
async def test_postgres_forum_crud():
    """测试 PostgreSQL Forum 表的 CRUD 操作"""
    dsn = _get_pg_dsn()
    engine = create_async_engine(dsn, echo=False)
    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    test_fid = 99999
    test_fname = "ci-forum-test"

    try:
        # 创建表
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # 插入
        async with session_maker() as sess:
            sess.add(Forum(fid=test_fid, fname=test_fname))
            await sess.commit()

        # 查询
        async with session_maker() as sess:
            res = await sess.execute(select(Forum).where(Forum.fid == test_fid))
            forum = res.scalar_one()
            assert forum.fname == test_fname

        # 删除（清理）
        async with session_maker() as sess:
            res = await sess.execute(select(Forum).where(Forum.fid == test_fid))
            forum = res.scalar_one_or_none()
            if forum:
                await sess.delete(forum)
                await sess.commit()

    finally:
        await engine.dispose()


@pytest.mark.skipif(not ON_CI, reason="Integration test only runs on CI")
@pytest.mark.asyncio
async def test_redis_id_queue_operations():
    """测试 Redis ID 队列操作"""
    consumer_cfg = ConsumerConfig(
        transport="redis",
        mode="id",
        max_len=3,
        id_queue_key="ci:test:id:queue",
        stream_prefix="ci:test:events",
        timeout_ms=500,
        max_retries=3,
        retry_backoff_ms=50,
    )

    rurl = _get_redis_url()
    r = redis.from_url(rurl, decode_responses=True)

    try:
        await r.ping()  # type: ignore

        pub = RedisStreamsPublisher(r, consumer_config=consumer_cfg)

        # 发送多条消息，测试 maxlen 限制
        for tid in (424240, 424241, 424242, 424243):
            await pub.publish_id("thread", tid)

        # 验证队列长度不超过 maxlen
        llen_raw = await r.execute_command("LLEN", consumer_cfg.id_queue_key)
        llen_int = 0 if llen_raw is None else int(llen_raw)
        assert 0 < llen_int <= consumer_cfg.max_len

        # 验证最新的消息在队列头部
        raw_queue_entries = await r.execute_command("LRANGE", consumer_cfg.id_queue_key, 0, -1)
        queue_entries = cast("list[str | bytes] | None", raw_queue_entries) or []
        ids_in_queue: list[int] = []
        for entry in queue_entries:
            if isinstance(entry, bytes):
                entry = entry.decode("utf-8")
            ids_in_queue.append(json.loads(entry)["id"])

        assert ids_in_queue[0] == 424243  # 最新的在头部

    finally:
        # 清理
        try:
            await r.delete(consumer_cfg.id_queue_key)
            aclose = getattr(r, "aclose", None)
            if callable(aclose):
                await aclose()
            else:
                close = getattr(r, "close", None)
                if callable(close):
                    close()
        except Exception:
            pass


@pytest.mark.skipif(not ON_CI, reason="Integration test only runs on CI")
@pytest.mark.asyncio
async def test_redis_stream_operations():
    """测试 Redis Stream 操作"""
    consumer_cfg = ConsumerConfig(
        transport="redis",
        mode="object",
        max_len=3,
        id_queue_key="ci:test:id:queue",
        stream_prefix="ci:test:events",
        timeout_ms=500,
        max_retries=3,
        retry_backoff_ms=50,
    )

    rurl = _get_redis_url()
    r = redis.from_url(rurl, decode_responses=True)

    try:
        await r.ping()  # type: ignore

        pub = RedisStreamsPublisher(r, consumer_config=consumer_cfg)

        # 发送第一个事件
        env1 = EventEnvelope(
            schema="tieba.thread.v1",
            type="upsert",
            object_type="thread",
            fid=1,
            object_id=424242,
            time=int(datetime.now(UTC).timestamp() * 1000),
            source="tests",
            backfill=False,
            payload={"tid": 424242, "fid": 1, "contents": {}},
        )
        await pub.publish_object(env1)

        # 发送第二个事件（带 backfill 标记）
        env2 = EventEnvelope(
            schema="tieba.thread.v1",
            type="upsert",
            object_type="thread",
            fid=1,
            object_id=424243,
            time=int(datetime.now(UTC).timestamp() * 1000),
            source="tests",
            backfill=True,
            payload={"tid": 424243, "fid": 1, "contents": {"message": "hello"}},
        )
        await pub.publish_object(env2)

        # 验证 Stream 中的数据
        stream_key = f"{consumer_cfg.stream_prefix}:1"
        entries = await r.xrevrange(stream_key, count=2)
        assert entries, "expected Redis stream entries to exist"

        # 验证最新的条目
        newest_entry = entries[0][1]["data"]
        if isinstance(newest_entry, bytes):
            newest_entry = newest_entry.decode("utf-8")
        payload = json.loads(newest_entry)

        assert payload["object_id"] == 424243
        assert payload["backfill"] is True
        assert payload["payload"]["contents"] == {"message": "hello"}

        # 验证 Stream 长度不超过 maxlen
        xlen = await r.xlen(stream_key)
        assert xlen <= consumer_cfg.max_len + 1

    finally:
        # 清理
        try:
            await r.delete(consumer_cfg.id_queue_key)
            await r.delete(f"{consumer_cfg.stream_prefix}:1")
            aclose = getattr(r, "aclose", None)
            if callable(aclose):
                await aclose()
            else:
                close = getattr(r, "close", None)
                if callable(close):
                    close()
        except Exception:
            pass


@pytest.mark.skipif(not ON_CI, reason="Integration test only runs on CI")
@pytest.mark.asyncio
async def test_postgres_and_redis_combined():
    """测试 PostgreSQL 和 Redis 组合操作"""
    dsn = _get_pg_dsn()
    engine = create_async_engine(dsn, echo=False)
    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    consumer_cfg = ConsumerConfig(
        transport="redis",
        mode="id",
        max_len=10,
        id_queue_key="ci:combined:queue",
        stream_prefix="ci:combined:events",
        timeout_ms=500,
        max_retries=3,
        retry_backoff_ms=50,
    )

    rurl = _get_redis_url()
    r = redis.from_url(rurl, decode_responses=True)

    test_fid = 88888
    test_fname = "ci-combined-forum"

    try:
        # 创建数据库表
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        await r.ping()  # type: ignore

        pub = RedisStreamsPublisher(r, consumer_config=consumer_cfg)

        # 1. 写入数据库
        async with session_maker() as sess:
            sess.add(Forum(fid=test_fid, fname=test_fname))
            await sess.commit()

        # 2. 推送到 Redis
        await pub.publish_id("forum", test_fid)

        # 3. 验证数据库
        async with session_maker() as sess:
            res = await sess.execute(select(Forum).where(Forum.fid == test_fid))
            forum = res.scalar_one()
            assert forum.fname == test_fname

        # 4. 验证 Redis
        raw_entry = await r.execute_command("LINDEX", consumer_cfg.id_queue_key, 0)
        if isinstance(raw_entry, bytes):
            raw_entry = raw_entry.decode("utf-8")
        assert raw_entry is not None
        entry = json.loads(raw_entry)
        assert entry["type"] == "forum"
        assert entry["id"] == test_fid

    finally:
        # 清理数据库
        try:
            async with session_maker() as sess:
                res = await sess.execute(select(Forum).where(Forum.fid == test_fid))
                forum = res.scalar_one_or_none()
                if forum:
                    await sess.delete(forum)
                    await sess.commit()
        except Exception:
            pass

        await engine.dispose()

        # 清理 Redis
        try:
            await r.delete(consumer_cfg.id_queue_key)
            aclose = getattr(r, "aclose", None)
            if callable(aclose):
                await aclose()
            else:
                close = getattr(r, "close", None)
                if callable(close):
                    close()
        except Exception:
            pass
