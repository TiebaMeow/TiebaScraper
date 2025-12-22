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


@pytest.mark.skipif(not ON_CI, reason="Integration test only runs on CI")
@pytest.mark.asyncio
async def test_postgres_and_redis_live_roundtrip():
    pg_user = os.getenv("PGUSER") or os.getenv("DB_USER", "postgres")
    pg_password = os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD", "postgres")
    pg_host = os.getenv("PGHOST") or os.getenv("DB_HOST", "127.0.0.1")
    pg_port = int(os.getenv("PGPORT") or os.getenv("DB_PORT", "5432"))
    pg_db = os.getenv("PGDATABASE") or os.getenv("DB_NAME", "tiebascraper")

    dsn = f"postgresql+asyncpg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_async_engine(dsn, echo=False)
    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    consumer_cfg = ConsumerConfig(
        transport="redis",
        mode="id",
        max_len=3,
        id_queue_key="ci:queue",
        stream_prefix="ci:events",
        timeout_ms=500,
        max_retries=3,
        retry_backoff_ms=50,
    )

    try:
        # create tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # insert one forum and read back
        async with session_maker() as sess:
            sess.add(Forum(fid=99999, fname="ci-forum"))
            await sess.commit()

        async with session_maker() as sess:
            res = await sess.execute(select(Forum).where(Forum.fid == 99999))
            forum = res.scalar_one()
            assert forum.fname == "ci-forum"

        # redis stream and list publish
        rurl = f"redis://{os.getenv('REDIS_HOST', '127.0.0.1')}:{int(os.getenv('REDIS_PORT', '6379'))}/0"
        r = redis.from_url(rurl, decode_responses=True)
        try:
            await r.ping()

            pub = RedisStreamsPublisher(
                r,
                consumer_config=consumer_cfg,
            )

            # ID queue should respect configured max length via ltrim
            for tid in (424240, 424241, 424242, 424243):
                await pub.publish_id("thread", tid)

            llen_raw = await r.execute_command("LLEN", consumer_cfg.id_queue_key)
            llen_int = 0 if llen_raw is None else int(llen_raw)
            assert 0 < llen_int <= consumer_cfg.max_len

            raw_queue_entries = await r.execute_command("LRANGE", consumer_cfg.id_queue_key, 0, -1)
            queue_entries = cast("list[str | bytes] | None", raw_queue_entries) or []
            ids_in_queue: list[int] = []
            for entry in queue_entries:
                if isinstance(entry, bytes):
                    entry = entry.decode("utf-8")
                ids_in_queue.append(json.loads(entry)["id"])
            assert ids_in_queue[0] == 424243  # newest at head

            env = EventEnvelope(
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
            await pub.publish_object(env)

            # publish additional events to ensure newest entry persists
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

            stream_key = f"{consumer_cfg.stream_prefix}:1:thread"
            entries = await r.xrevrange(stream_key, count=2)
            assert entries, "expected Redis stream entries to exist"

            newest_entry = entries[0][1]["data"]
            if isinstance(newest_entry, bytes):
                newest_entry = newest_entry.decode("utf-8")
            payload = json.loads(newest_entry)
            assert payload["object_id"] == 424243
            assert payload["backfill"] is True
            assert payload["payload"]["contents"] == {"message": "hello"}

            # ensure stream length does not exceed configured max len by a large margin
            xlen = await r.xlen(stream_key)
            assert xlen <= consumer_cfg.max_len + 1
        finally:
            # best-effort close for different redis.asyncio versions
            try:
                await r.delete(consumer_cfg.id_queue_key)
                await r.delete(f"{consumer_cfg.stream_prefix}:1:thread")
                aclose = getattr(r, "aclose", None)
                if callable(aclose):
                    await aclose()  # type: ignore[misc]
                else:
                    close = getattr(r, "close", None)
                    if callable(close):
                        close()
            except Exception:
                pass
    finally:
        await engine.dispose()
