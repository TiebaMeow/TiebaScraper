"""DataStore 核心功能测试。"""

import json
from datetime import datetime
from typing import Any, cast

import pytest
from sqlalchemy.dialects import postgresql
from tiebameow.models.dto import BaseThreadDTO, ThreadDTO, ThreadUserDTO

from src.core.config import ConsumerConfig
from src.core.datastore import DataStore

# ==================== Mock 类 ====================


class DummyCache:
    """测试用缓存模拟类"""

    def __init__(self) -> None:
        self._kv: dict[str, object] = {}

    async def get_many(self, *keys: str) -> tuple[object | None, ...]:
        return tuple(self._kv.get(k) for k in keys)

    async def set_many(self, mapping: dict[str, object], expire: int | float | None = None) -> None:
        self._kv.update(mapping)


class DummyRedis:
    """测试用 Redis 模拟类"""

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

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        return True

    async def delete(self, key: str) -> int:
        return 1


class DummySession:
    """测试用数据库会话模拟类"""

    def __init__(self, existing_ids: set[int] | None = None) -> None:
        self.existing_ids = existing_ids or set()
        self._tx_committed = False
        self._items_added: list[Any] = []
        self.last_statement: object | None = None

    async def execute(self, statement: object):
        self.last_statement = statement

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
    """测试用会话工厂模拟类"""

    def __init__(self, session: DummySession) -> None:
        self._session = session

    def __call__(self) -> "DummySessionMaker":
        return self

    async def __aenter__(self) -> DummySession:
        return self._session

    async def __aexit__(self, *exc: object) -> bool:
        return False


class DummyConfig:
    """测试用配置模拟类"""

    def __init__(self, transport: str = "redis") -> None:
        self.cache_backend = "memory"
        self.cache_max_size = 100_000
        self.cache_ttl_seconds = 86400
        self.redis_url = "redis://localhost:6379/0"
        self.websocket_enabled = False
        self.websocket_url = "ws://127.0.0.1:0/ws"

        self._consumer_config = ConsumerConfig(
            transport=transport,  # type: ignore
            max_len=16,
            stream_prefix="test:stream",
        )

    @property
    def consumer_transport(self) -> str:
        return self._consumer_config.transport

    @property
    def consumer_config(self) -> ConsumerConfig:
        return self._consumer_config


class DummyContainer:
    """测试用容器模拟类"""

    def __init__(
        self,
        session: DummySession,
        redis_client: DummyRedis,
        config: DummyConfig | None = None,
    ) -> None:
        self.redis_client = redis_client
        self.async_sessionmaker = DummySessionMaker(session)
        self.config = config or DummyConfig()
        self.ws_server = None


# ==================== 缓存过滤测试 ====================


@pytest.mark.asyncio
async def test_filter_by_cache_and_mark_processed():
    """测试缓存过滤和标记已处理"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 第一次过滤，所有 ID 都应该是新的
    new = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new == {1, 2, 3}

    # 标记为已处理
    await ds._mark_as_processed("thread", new)

    # 第二次过滤，应该都被过滤掉
    new_again = await ds._filter_by_cache("thread", [1, 2, 3])
    assert new_again == set()


@pytest.mark.asyncio
async def test_filter_by_database():
    """测试数据库过滤"""
    DataStore._cache = None

    # 模拟数据库中已存在 ID 1 和 2
    dummy_session = DummySession(existing_ids={1, 2})
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 只有 ID 3 应该是新的
    new = await ds._filter_by_database("thread", [1, 2, 3])
    assert new == {3}


@pytest.mark.asyncio
async def test_filter_new_ids_full_flow():
    """测试完整的 filter_new_ids 流程"""
    DataStore._cache = None

    # 数据库中已存在 ID 1
    dummy_session = DummySession(existing_ids={1})
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 第一次调用：ID 2 和 3 应该是新的
    new = await ds.filter_new_ids("thread", [1, 2, 3])
    assert new == {2, 3}

    # 第二次调用：ID 2 和 3 已被缓存标记，应该返回空
    # 注意：由于缓存标记，即使数据库中不存在，也会被认为已处理
    new_again = await ds.filter_new_ids("thread", [2, 3])
    assert new_again == set()


@pytest.mark.asyncio
async def test_filter_new_ids_with_duplicates():
    """测试 filter_new_ids 处理重复 ID"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    # 包含重复 ID
    new = await ds.filter_new_ids("thread", [1, 1, 2, 2, 3])
    assert new == {1, 2, 3}


# ==================== 消息推送测试 ====================


@pytest.mark.asyncio
async def test_push_object_event():
    """测试推送对象事件到 Redis Stream"""
    DataStore._cache = None

    session = DummySession(existing_ids=set())
    redis = DummyRedis()
    container = DummyContainer(session, redis, DummyConfig(transport="redis"))

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    user = ThreadUserDTO(
        user_id=1,
        portrait="p",
        user_name="u",
        nick_name_new="n",
        level=1,
        glevel=1,
        gender="UNKNOWN",
        icons=[],
        is_bawu=False,
        is_vip=False,
        is_god=False,
        priv_like="PUBLIC",
        priv_reply="ALL",
    )
    obj = ThreadDTO(
        tid=77,
        fid=1,
        fname="f",
        pid=77,
        author_id=1,
        author=user,
        title="t",
        contents=[],
        is_good=False,
        is_top=False,
        is_share=False,
        is_hide=False,
        is_livepost=False,
        is_help=False,
        agree_num=0,
        disagree_num=0,
        reply_num=0,
        view_num=0,
        share_num=0,
        create_time=datetime.fromtimestamp(0),
        last_time=datetime.fromtimestamp(0),
        thread_type=0,
        tab_id=0,
        share_origin=BaseThreadDTO(pid=0, tid=0, fid=0, fname="", author_id=0, title="", contents=[]),
    )

    await ds.push_object_event("thread", obj)

    # 验证消息被推送到 Redis Stream
    stream_key = f"{container.config.consumer_config.stream_prefix}:1"
    assert stream_key in redis.streams
    payload = redis.streams[stream_key][0]
    assert payload.startswith(b"{")

    # 验证 payload 内容
    data = json.loads(payload)
    assert data["object_type"] == "thread"
    assert data["object_id"] == 77
    assert data["fid"] == 1


@pytest.mark.asyncio
async def test_push_object_event_noop_when_transport_none():
    """测试 transport=none 时 push_object_event 是 no-op"""
    DataStore._cache = None

    session = DummySession(existing_ids=set())
    redis = DummyRedis()
    container = DummyContainer(session, redis, DummyConfig(transport="none"))

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    user = ThreadUserDTO(
        user_id=1,
        portrait="p",
        user_name="u",
        nick_name_new="n",
        level=1,
        glevel=1,
        gender="UNKNOWN",
        icons=[],
        is_bawu=False,
        is_vip=False,
        is_god=False,
        priv_like="PUBLIC",
        priv_reply="ALL",
    )
    obj = ThreadDTO(
        tid=77,
        fid=1,
        fname="f",
        pid=77,
        author_id=1,
        author=user,
        title="t",
        contents=[],
        is_good=False,
        is_top=False,
        is_share=False,
        is_hide=False,
        is_livepost=False,
        is_help=False,
        agree_num=0,
        disagree_num=0,
        reply_num=0,
        view_num=0,
        share_num=0,
        create_time=datetime.fromtimestamp(0),
        last_time=datetime.fromtimestamp(0),
        thread_type=0,
        tab_id=0,
        share_origin=BaseThreadDTO(pid=0, tid=0, fid=0, fname="", author_id=0, title="", contents=[]),
    )

    await ds.push_object_event("thread", obj)

    # transport=none 时不应该推送到 Stream
    stream_key = f"{container.config.consumer_config.stream_prefix}:1"
    assert stream_key not in redis.streams


@pytest.mark.asyncio
async def test_get_pending_thread_scan_tids_returns_existing_subset():
    """测试 get_pending_thread_scan_tids 返回待扫描 tid 子集。"""
    DataStore._cache = None

    # DummySession.fetchall 会返回 existing_ids 中的值
    dummy_session = DummySession(existing_ids={1, 3})
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    pending = await ds.get_pending_thread_scan_tids({1, 2, 3, 4})
    assert pending == {1, 3}


# ==================== 边界情况测试 ====================


@pytest.mark.asyncio
async def test_filter_new_ids_empty_list():
    """测试空列表输入"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    new = await ds.filter_new_ids("thread", [])
    assert new == set()


@pytest.mark.asyncio
async def test_mark_as_processed_empty_set():
    """测试空集合标记"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    cache = DummyCache()
    ds.cache = cast("Any", cache)

    # 空集合不应该写入缓存
    await ds._mark_as_processed("thread", set())
    assert len(cache._kv) == 0


@pytest.mark.asyncio
async def test_add_pending_comment_scan_uses_conflict_update_for_task_kind_upgrade():
    """测试 pending_comment_scan 在主键冲突时会更新 task_kind 与 backfill。"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    await ds.add_pending_comment_scan(tid=1, pid=2, backfill=False, task_kind="full")

    assert dummy_session.last_statement is not None
    compiled = str(
        dummy_session.last_statement.compile(  # type: ignore[attr-defined]
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "ON CONFLICT" in compiled
    assert "DO UPDATE" in compiled
    assert "task_kind" in compiled
    assert "backfill" in compiled


@pytest.mark.asyncio
async def test_remove_pending_comment_scan_supports_task_kind_filter():
    """测试 remove_pending_comment_scan 支持按 task_kind 条件删除。"""
    DataStore._cache = None

    dummy_session = DummySession(existing_ids=set())
    dummy_redis = DummyRedis()
    container = DummyContainer(dummy_session, dummy_redis)

    ds = DataStore(cast("Any", container))
    ds.cache = cast("Any", DummyCache())

    await ds.remove_pending_comment_scan(1, 2, task_kind="incremental")

    assert dummy_session.last_statement is not None
    compiled = str(
        dummy_session.last_statement.compile(  # type: ignore[attr-defined]
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "DELETE FROM pending_comment_scan" in compiled
    assert "task_kind" in compiled
