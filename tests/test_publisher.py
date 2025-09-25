from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from src.config import ConsumerConfig
from src.core.publisher import RedisStreamsPublisher, build_envelope


@dataclass
class _User:
    user_id: int


@dataclass
class _Thread:
    tid: int
    fid: int
    fname: str
    title: str = "t"
    text: str = "tx"
    last_time: int = 0
    reply_num: int = 0
    user: _User = field(default_factory=lambda: _User(1))
    contents: object = None


class DummyRedis:
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []
        self._fail_times = 0

    async def wait(self, *args, **kwargs):
        return 1

    def set_fail_times(self, n: int):
        self._fail_times = n

    async def lpush(self, key, value):
        self.calls.append(("lpush", (key, value), {}))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("transient")
        return 1

    async def xadd(self, stream, entry, maxlen=None, approximate=None):
        self.calls.append(("xadd", (stream, entry), {"maxlen": maxlen, "approximate": approximate}))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("transient")
        return "0-1"


@pytest.mark.asyncio
async def test_build_envelope_and_publish_with_retry():
    redis = DummyRedis()
    pub = RedisStreamsPublisher(
        cast("Any", redis),
        consumer_config=ConsumerConfig(
            max_len=10,
            timeout_ms=50,
            max_retries=3,
            retry_backoff_ms=1,
            id_queue_key="q",
        ),
    )

    t = _Thread(tid=11, fid=22, fname="bar")
    env = build_envelope("thread", cast("Any", t), event_type="upsert", backfill=False)
    assert env.object_id == 11
    assert env.fid == 22
    assert env.object_type == "thread"

    # make first call fail and retry succeeds
    redis.set_fail_times(1)
    await pub.publish_id("thread", 11)
    assert any(c[0] == "lpush" for c in redis.calls)

    redis.set_fail_times(2)
    await pub.publish_object(env)
    assert any(c[0] == "xadd" for c in redis.calls)
