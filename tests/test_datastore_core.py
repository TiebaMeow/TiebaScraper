from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.core.datastore import DataStore


class DummyCache:
    def __init__(self):
        self._kv: dict[str, object] = {}

    async def get_many(self, *keys):
        return {k: self._kv[k] for k in keys if k in self._kv}

    async def set_many(self, d: dict[str, object], expire: str):
        self._kv.update(d)


class DummyRedis:
    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.streams: dict[str, list[bytes]] = {}

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def xadd(self, stream, entry, maxlen=None, approximate=None):
        self.streams.setdefault(stream, []).append(entry["data"])  # type: ignore[index]
        return f"0-{len(self.streams[stream])}"

    async def wait(self, n, timeout_ms):
        return 1


class DummySession:
    def __init__(self, existing_ids: set[int]):
        self.existing_ids = existing_ids
        self._tx_committed = False

    async def execute(self, statement):
        class Result:
            def __init__(self, ids):
                self._ids = ids

            def fetchall(self):
                return [(i,) for i in self._ids]

            def scalars(self):
                return self

            def all(self):
                return []

        # Simpler: our test paths don't rely on the actual SQL; just return configured ids
        return Result(self.existing_ids)

    async def commit(self):
        self._tx_committed = True

    async def rollback(self):
        pass


class DummySessionMaker:
    def __init__(self, session: DummySession):
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *exc):
        return False


class DummyConfig:
    consumer_mode: str = "id"
    consumer_object_prefix: str = "s"
    consumer_object_maxlen: int = 100
    consumer_object_approx: bool = True
    consumer_json_compact: bool = True
    consumer_publish_timeout_ms: int = 100
    consumer_publish_max_retries: int = 1
    consumer_publish_retry_backoff_ms: int = 1
    consumer_id_queue_key: str = "q"
    redis_url: str = "redis://localhost:6379/0"


class DummyContainer:
    def __init__(self, session: DummySession, redis_client: DummyRedis):
        self.redis_client = redis_client
        self.async_sessionmaker = DummySessionMaker(session)
        self.config = DummyConfig()


@pytest.mark.asyncio
async def test_filter_by_cache_and_mark_processed():
    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    # swap out networked cache with in-memory dummy
    ds.cache = cast("Any", DummyCache())

    new = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new == {1, 2, 3}
    await ds._mark_as_processed("thread", new)
    # second time should be filtered out by cache
    new2 = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new2 == set()


@pytest.mark.asyncio
async def test_push_to_id_and_object_events():
    session = DummySession(existing_ids=set())
    redis = DummyRedis()
    container = DummyContainer(session, redis)
    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # id mode
    await ds.push_to_id_queue("post", 123)
    assert redis.lists[container.config.consumer_id_queue_key]

    # switch to object mode and publish an envelope with minimal object stub
    container.config.consumer_mode = "object"  # type: ignore[attr-defined]
    ds2 = DataStore(cast("Any", container))
    ds2.cache = cast("Any", DummyCache())

    obj = SimpleNamespace(fid=1, tid=77, pid=0, contents=None)
    await ds2.push_object_event("thread", obj)  # type: ignore[arg-type]
    # expect one stream created
    assert any(k.startswith("s:1:thread") for k in redis.streams)
