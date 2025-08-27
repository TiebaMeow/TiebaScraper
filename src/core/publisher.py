"""事件发布模块（对象直推）。

提供统一的发布接口与 Redis Streams 实现，用于将 Thread/Post/Comment 的对象事件
以 JSON 形式写入 Redis Streams。支持重试与配置化的 stream 前缀。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, cast
from zoneinfo import ZoneInfo

from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from ..utils import to_jsonable

try:
    import orjson as _orjson
except Exception:
    _orjson = None

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import aiotieba.typing as aiotieba
    import redis.asyncio as redis

    AiotiebaType = aiotieba.Thread | aiotieba.Post | aiotieba.Comment


log = logging.getLogger("publisher")

ItemType = Literal["thread", "post", "comment"]

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _now_ms() -> int:
    return int(datetime.now(SHANGHAI_TZ).timestamp() * 1000)


@dataclass
class EventEnvelope:
    schema: str
    type: str
    id: int
    ts: int
    source: str
    backfill: bool
    payload: dict[str, Any]

    def to_json_bytes(self, compact: bool = True) -> bytes:
        try:
            if _orjson is not None:
                return _orjson.dumps(self.__dict__, option=(_orjson.OPT_SORT_KEYS if compact else 0))
            raise RuntimeError("orjson not available")
        except Exception as e:
            log.exception(f"orjson serialization failed, fallback to json: {e}")
            return json.dumps(self.__dict__, separators=(",", ":") if compact else None).encode("utf-8")


class Publisher:
    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        raise NotImplementedError

    async def publish_object(self, item_type: ItemType, envelope: EventEnvelope) -> None:
        raise NotImplementedError


class NoopPublisher(Publisher):
    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        return

    async def publish_object(self, item_type: ItemType, envelope: EventEnvelope) -> None:
        return


class RedisStreamsPublisher(Publisher):
    """基于 Redis Streams 的对象事件发布器。"""

    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        stream_prefix: str = "events",
        maxlen: int = 10000,
        approx: bool = True,
        json_compact: bool = True,
        timeout_ms: int = 2000,
        max_retries: int = 5,
        retry_backoff_ms: int = 200,
        id_queue_key: str = "consumer:queue",
    ) -> None:
        self.redis = redis_client
        self.stream_prefix = stream_prefix
        self.maxlen = maxlen
        self.approx = approx
        self.json_compact = json_compact
        self.timeout_ms = timeout_ms
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.id_queue_key = id_queue_key

    async def _retry(self, func: Callable[[], Awaitable[Any]], *, fail_log_msg: str) -> Any:
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.max_retries),
                wait=wait_fixed(max(self.retry_backoff_ms, 0) / 1000.0),
                retry=retry_if_exception_type(Exception),
                reraise=True,
            ):
                with attempt:
                    try:
                        return await func()
                    except Exception:
                        try:
                            await self.redis.wait(1, self.timeout_ms)
                        except Exception:
                            pass
                        raise
        except Exception as e:
            log.exception(f"{fail_log_msg} after {self.max_retries} retries: {e}")
            raise

    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        payload = {"type": item_type, "id": int(item_id)}
        try:
            if _orjson is not None:
                message = _orjson.dumps(payload).decode("utf-8")
            else:
                message = json.dumps(payload, separators=(",", ":") if self.json_compact else None)
        except Exception:
            message = f'{{"type":"{item_type}","id":{int(item_id)}}}'

        await self._retry(
            lambda: self.redis.lpush(self.id_queue_key, message),  # type: ignore
            fail_log_msg=f"Failed to enqueue to list={self.id_queue_key}",
        )

    async def publish_object(self, item_type: ItemType, envelope: EventEnvelope) -> None:
        stream = f"{self.stream_prefix}:{item_type}"
        data = envelope.to_json_bytes(self.json_compact)

        entry = {"data": data}

        await self._retry(
            lambda: self.redis.xadd(
                stream,
                cast("Any", entry),
                maxlen=self.maxlen,
                approximate=self.approx,
            ),
            fail_log_msg=f"Failed to publish to stream={stream}",
        )


def build_envelope(
    item_type: ItemType,
    obj: AiotiebaType,
    *,
    event_type: str = "upsert",
    backfill: bool = False,
) -> EventEnvelope:
    schema = f"tieba.{item_type}.v1"
    if item_type == "thread":
        eid = obj.tid
    else:
        eid = obj.pid
    payload = to_jsonable(obj)

    return EventEnvelope(
        schema=schema,
        type=f"{item_type}.{event_type}",
        id=eid,
        ts=_now_ms(),
        source="scraper",
        backfill=backfill,
        payload=payload,
    )
