from datetime import datetime
from typing import Any, cast

import pytest
from tiebameow.models.dto import BaseThreadDTO, ThreadDTO, ThreadUserDTO

from src.core.config import ConsumerConfig
from src.core.publisher import RedisStreamsPublisher, build_envelope


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

    t = ThreadDTO(
        tid=11,
        fid=22,
        fname="bar",
        pid=11,
        author_id=1,
        author=ThreadUserDTO(
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
        ),
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
