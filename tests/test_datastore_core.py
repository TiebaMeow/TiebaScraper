import json
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.config import ConsumerConfig
from src.core.datastore import DataStore


class DummyCache:
    def __init__(self) -> None:
        self._kv: dict[str, object] = {}

    async def get_many(self, *keys: str) -> dict[str, object]:
        return {k: self._kv[k] for k in keys if k in self._kv}

    async def set_many(self, mapping: dict[str, object], expire: int | float | None = None) -> None:
        self._kv.update(mapping)


class DummyRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.streams: dict[str, list[bytes]] = {}

    async def lpush(self, key: str, value: str) -> int:
        lst = self.lists.setdefault(key, [])
        lst.insert(0, value)
        return len(lst)

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        if key not in self.lists:
            return
        self.lists[key] = self.lists[key][start : stop + 1 if stop >= 0 else None]

    async def xadd(
        self,
        stream: str,
        entry: dict[str, bytes],
        maxlen: int | None = None,
        approximate: bool | None = None,
    ) -> str:
        bucket = self.streams.setdefault(stream, [])
        bucket.append(entry["data"])
        if maxlen is not None and len(bucket) > maxlen:
            del bucket[:-maxlen]
        return f"0-{len(bucket)}"

    async def wait(self, n: int, timeout_ms: int) -> int:
        return 1


class DummySession:
    def __init__(self, existing_ids: set[int]) -> None:
        self.existing_ids = existing_ids
        self._tx_committed = False

    async def execute(self, statement: object):
        class Result:
            def __init__(self, ids: set[int]) -> None:
                self._ids = ids

            def fetchall(self) -> list[tuple[int]]:
                return [(i,) for i in self._ids]

            def scalars(self) -> "Result":
                return self

            def all(self) -> list[Any]:
                return []

        return Result(self.existing_ids)

    async def commit(self) -> None:
        self._tx_committed = True

    async def rollback(self) -> None:
        pass


class DummySessionMaker:
    def __init__(self, session: DummySession) -> None:
        self._session = session

    def __call__(self) -> "DummySessionMaker":
        return self

    async def __aenter__(self) -> DummySession:
        return self._session

    async def __aexit__(self, *exc: object) -> bool:
        return False


class DummyConfig:
    def __init__(self) -> None:
        self.cache_backend = "memory"
        self.cache_max_size = 100_000
        self.cache_ttl_seconds = 86400

        self._consumer_config = ConsumerConfig(
            transport="redis",
            mode="id",
            max_len=16,
            id_queue_key="test:id:queue",
            stream_prefix="test:stream",
            timeout_ms=200,
            max_retries=3,
            retry_backoff_ms=10,
        )

        self.consumer_transport = "redis"
        self.redis_url = "redis://localhost:6379/0"
        self.websocket_enabled = False
        self.websocket_url = "ws://127.0.0.1:0/ws"

        self.consumer_mode = "id"

    @property
    def consumer_config(self) -> ConsumerConfig:
        return self._consumer_config

    @property
    def consumer_mode(self) -> str:
        return self._consumer_config.mode

    @consumer_mode.setter
    def consumer_mode(self, value: str) -> None:
        self._consumer_config.mode = value  # type: ignore[assignment]


class DummyContainer:
    def __init__(self, session: DummySession, redis_client: DummyRedis) -> None:
        self.redis_client = redis_client
        self.async_sessionmaker = DummySessionMaker(session)
        self.config = DummyConfig()


@pytest.mark.asyncio
async def test_filter_by_cache_and_mark_processed():
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    new = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new == {1, 2, 3}

    await ds._mark_as_processed("thread", new)

    new_again = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new_again == set()


@pytest.mark.asyncio
async def test_push_to_id_and_object_events():
    DataStore._cache = None

    session = DummySession(existing_ids=set())
    redis = DummyRedis()
    container = DummyContainer(session, redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    await ds.push_to_id_queue("post", 123)
    assert container.config.consumer_config.id_queue_key in redis.lists
    queued = redis.lists[container.config.consumer_config.id_queue_key][0]
    assert json.loads(queued) == {"type": "post", "id": 123}

    container.config.consumer_mode = "object"

    ds_object = DataStore(cast("Any", container))
    ds_object.cache = cast("Any", DummyCache())

    obj = SimpleNamespace(fid=1, tid=77, pid=0, cid=0)
    await ds_object.push_object_event("thread", obj)  # type: ignore[arg-type]

    stream_key = f"{container.config.consumer_config.stream_prefix}:1:thread"
    assert stream_key in redis.streams
    payload = redis.streams[stream_key][0]
    assert payload.startswith(b"{")
