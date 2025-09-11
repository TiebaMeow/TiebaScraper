import os
from datetime import UTC, datetime

import pytest
import redis.asyncio as redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.publisher import EventEnvelope, RedisStreamsPublisher
from src.models.models import Base, Forum

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
        r = redis.from_url(rurl, decode_responses=False)
        try:
            await r.ping()

            pub = RedisStreamsPublisher(
                r,
                stream_prefix="ci:events",
                maxlen=10,
                approx=True,
                json_compact=True,
                timeout_ms=500,
                max_retries=3,
                retry_backoff_ms=50,
                id_queue_key="ci:queue",
            )

            await pub.publish_id("thread", 424242)
            # use execute_command to avoid type-checker false positives on awaitability
            llen = await r.execute_command("LLEN", "ci:queue")
            assert int(llen or 0) >= 1

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
            # confirm entry exists
            xlen = await r.execute_command("XLEN", "ci:events:1:thread")
            assert int(xlen or 0) >= 1
        finally:
            # best-effort close for different redis.asyncio versions
            try:
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
