"""Redis Streams Publisher 测试。"""

from datetime import datetime
from typing import Any, cast

import pytest
from tiebameow.models.dto import BaseThreadDTO, ThreadDTO, ThreadUserDTO

from src.core.config import ConsumerConfig
from src.core.publisher import EventEnvelope, RedisStreamsPublisher, build_envelope

# ==================== Mock 类 ====================


class DummyRedis:
    """测试用 Redis 模拟类"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self._fail_times = 0

    async def wait(self, *args, **kwargs):
        return 1

    def set_fail_times(self, n: int):
        """设置前 n 次调用失败"""
        self._fail_times = n

    async def lpush(self, key, value):
        self.calls.append(("lpush", (key, value), {}))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("transient")
        return 1

    async def ltrim(self, key, start, stop):
        self.calls.append(("ltrim", (key, start, stop), {}))
        return None

    async def xadd(self, stream, entry, maxlen=None, approximate=None):
        self.calls.append(("xadd", (stream, entry), {"maxlen": maxlen, "approximate": approximate}))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("transient")
        return "0-1"


# ==================== build_envelope 测试 ====================


def test_build_envelope_thread():
    """测试 build_envelope 构建 Thread 事件"""
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
    thread = ThreadDTO(
        tid=11,
        fid=22,
        fname="bar",
        pid=11,
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

    env = build_envelope("thread", cast("Any", thread), event_type="upsert", backfill=False)

    assert env.object_id == 11
    assert env.fid == 22
    assert env.object_type == "thread"
    assert env.schema == "tieba.thread.v1"
    assert env.type == "upsert"
    assert env.backfill is False
    assert env.source == "scraper"
    assert "tid" in env.payload


def test_build_envelope_backfill():
    """测试 build_envelope 构建回溯事件"""
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
    thread = ThreadDTO(
        tid=11,
        fid=22,
        fname="bar",
        pid=11,
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

    env = build_envelope("thread", cast("Any", thread), event_type="upsert", backfill=True)

    assert env.backfill is True


# ==================== EventEnvelope 测试 ====================


def test_event_envelope_to_json_bytes():
    """测试 EventEnvelope 序列化为 JSON bytes"""
    env = EventEnvelope(
        schema="tieba.thread.v1",
        type="upsert",
        object_type="thread",
        fid=1,
        object_id=123,
        time=1000000,
        source="scraper",
        backfill=False,
        payload={"tid": 123, "title": "test"},
    )

    json_bytes = env.to_json_bytes()

    assert isinstance(json_bytes, bytes)
    assert b"thread" in json_bytes
    assert b"123" in json_bytes


# ==================== RedisStreamsPublisher 测试 ====================


@pytest.mark.asyncio
async def test_redis_publisher_publish_id():
    """测试 RedisStreamsPublisher publish_id"""
    redis = DummyRedis()
    pub = RedisStreamsPublisher(
        cast("Any", redis),
        consumer_config=ConsumerConfig(
            max_len=10,
            timeout_ms=50,
            max_retries=3,
            retry_backoff_ms=1,
            id_queue_key="test:queue",
        ),
    )

    await pub.publish_id("thread", 11)

    # 验证 lpush 和 ltrim 被调用
    lpush_calls = [c for c in redis.calls if c[0] == "lpush"]
    ltrim_calls = [c for c in redis.calls if c[0] == "ltrim"]

    assert len(lpush_calls) == 1
    assert len(ltrim_calls) == 1
    assert lpush_calls[0][1][0] == "test:queue"


@pytest.mark.asyncio
async def test_redis_publisher_publish_object():
    """测试 RedisStreamsPublisher publish_object"""
    redis = DummyRedis()
    pub = RedisStreamsPublisher(
        cast("Any", redis),
        consumer_config=ConsumerConfig(
            max_len=10,
            timeout_ms=50,
            max_retries=3,
            retry_backoff_ms=1,
            stream_prefix="test:events",
        ),
    )

    env = EventEnvelope(
        schema="tieba.thread.v1",
        type="upsert",
        object_type="thread",
        fid=22,
        object_id=11,
        time=1000000,
        source="scraper",
        backfill=False,
        payload={"tid": 11},
    )

    await pub.publish_object(env)

    # 验证 xadd 被调用
    xadd_calls = [c for c in redis.calls if c[0] == "xadd"]
    assert len(xadd_calls) == 1
    assert xadd_calls[0][1][0] == "test:events:22"  # stream_prefix:fid


@pytest.mark.asyncio
async def test_redis_publisher_retry_on_failure():
    """测试 RedisStreamsPublisher 失败重试"""
    redis = DummyRedis()
    pub = RedisStreamsPublisher(
        cast("Any", redis),
        consumer_config=ConsumerConfig(
            max_len=10,
            timeout_ms=50,
            max_retries=5,
            retry_backoff_ms=1,
            id_queue_key="test:queue",
        ),
    )

    # 设置前2次调用失败
    redis.set_fail_times(2)

    await pub.publish_id("thread", 11)

    # 应该有3次 lpush 调用（2次失败 + 1次成功）
    lpush_calls = [c for c in redis.calls if c[0] == "lpush"]
    assert len(lpush_calls) == 3


@pytest.mark.asyncio
async def test_redis_publisher_retry_exhausted():
    """测试 RedisStreamsPublisher 重试耗尽"""
    redis = DummyRedis()
    pub = RedisStreamsPublisher(
        cast("Any", redis),
        consumer_config=ConsumerConfig(
            max_len=10,
            timeout_ms=50,
            max_retries=3,
            retry_backoff_ms=1,
            id_queue_key="test:queue",
        ),
    )

    # 设置所有调用失败
    redis.set_fail_times(100)

    with pytest.raises(RuntimeError):
        await pub.publish_id("thread", 11)


@pytest.mark.asyncio
async def test_redis_publisher_maxlen():
    """测试 RedisStreamsPublisher maxlen 配置"""
    redis = DummyRedis()
    pub = RedisStreamsPublisher(
        cast("Any", redis),
        consumer_config=ConsumerConfig(
            max_len=100,
            timeout_ms=50,
            max_retries=3,
            retry_backoff_ms=1,
            stream_prefix="test:events",
        ),
    )

    env = EventEnvelope(
        schema="tieba.thread.v1",
        type="upsert",
        object_type="thread",
        fid=1,
        object_id=11,
        time=1000000,
        source="scraper",
        backfill=False,
        payload={},
    )

    await pub.publish_object(env)

    # 验证 xadd 使用了 maxlen 参数
    xadd_calls = [c for c in redis.calls if c[0] == "xadd"]
    assert xadd_calls[0][2]["maxlen"] == 100
