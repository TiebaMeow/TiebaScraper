"""事件发布模块（对象直推）。

提供统一的发布接口与 Redis Streams 实现，用于将 Thread/Post/Comment 的对象事件
以 JSON 形式写入 Redis Streams。支持重试与配置化的 stream 前缀。
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, cast
from zoneinfo import ZoneInfo

from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential, wait_fixed
from tiebameow.utils.logger import logger

try:
    import orjson as _orjson
except Exception:
    _orjson = None

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import redis.asyncio as redis
    from pydantic import WebsocketUrl
    from tiebameow.models.dto import CommentDTO, PostDTO, ThreadDTO

    from .config import ConsumerConfig
    from .ws_server import WebSocketServer

    type DTOType = ThreadDTO | PostDTO | CommentDTO
    type ItemType = Literal["thread", "post", "comment"]


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _now_ms() -> int:
    return int(datetime.now(SHANGHAI_TZ).timestamp() * 1000)


@dataclass
class EventEnvelope:
    schema: str
    type: str
    object_type: str
    fid: int
    object_id: int
    time: int
    source: str
    backfill: bool
    payload: dict[str, Any]

    def to_json_bytes(self) -> bytes:
        try:
            if _orjson is not None:
                return _orjson.dumps(self.__dict__, option=_orjson.OPT_SERIALIZE_NUMPY)
            raise RuntimeError("orjson not available")
        except Exception as e:
            logger.exception("orjson serialization failed, fallback to json: {}", e)
            return json.dumps(self.__dict__).encode("utf-8")


class Publisher:
    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        raise NotImplementedError

    async def publish_object(self, envelope: EventEnvelope) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        """可选：关闭底层资源。默认无操作。"""
        return


class NoopPublisher(Publisher):
    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        return

    async def publish_object(self, envelope: EventEnvelope) -> None:
        return


class WebSocketPublisher(Publisher):
    """基于 WebSocket 的对象事件发布器（服务端广播）。

    需要传入一个已启动的 WebSocketServer 实例。
    内部维护一个异步队列与后台任务，负责将消息广播给所有连接的客户端。
    支持消息重试、队列容量限制（超出则丢弃最旧消息）等功能。
    """

    def __init__(
        self,
        url: WebsocketUrl,
        *,
        server: WebSocketServer | None,
        consumer_config: ConsumerConfig,
    ) -> None:
        if server is None:
            raise ValueError("WebSocketPublisher requires an existing WebSocketServer instance")

        self.url = url
        self.timeout_ms = consumer_config.timeout_ms
        self.max_retries = consumer_config.max_retries
        self.retry_backoff_ms = consumer_config.retry_backoff_ms
        self.queue_capacity = consumer_config.max_len

        self._server = server

        self._queue: deque[str] = deque()
        self._queue_lock = asyncio.Lock()
        self._message_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _ensure_worker(self) -> None:
        if self._worker_task is not None and not self._worker_task.done():
            return
        loop = self._loop
        if loop is None:
            loop = asyncio.get_running_loop()
            self._loop = loop
        self._worker_task = loop.create_task(self._worker_loop())

    async def _enqueue(self, message: str) -> None:
        if self._stop_event.is_set():
            logger.warning("WebSocketPublisher is closed; dropping message")
            return

        dropped: str | None = None
        async with self._queue_lock:
            if len(self._queue) >= self.queue_capacity:
                dropped = self._queue.popleft()
            self._queue.append(message)
            self._message_event.set()

        if dropped is not None:
            logger.warning("WebSocketPublisher queue full; dropped oldest message")

    async def _next_message(self) -> str | None:
        while True:
            async with self._queue_lock:
                if self._queue:
                    return self._queue.popleft()
                if self._stop_event.is_set():
                    return None
                self._message_event.clear()
            await self._message_event.wait()

    async def _requeue_front(self, message: str) -> None:
        if self._stop_event.is_set():
            return
        async with self._queue_lock:
            if len(self._queue) >= self.queue_capacity:
                logger.warning("WebSocketPublisher queue full; dropping message on requeue")
                return
            self._queue.appendleft(message)
            self._message_event.set()

    async def _send_with_retry(self, message: str) -> bool:
        base_wait = max(self.retry_backoff_ms, 10) / 1000.0
        max_wait = max(self.timeout_ms / 1000.0, base_wait) if self.timeout_ms > 0 else None
        wait_kwargs: dict[str, float] = {"multiplier": base_wait, "min": base_wait}
        if max_wait is not None:
            wait_kwargs["max"] = max_wait

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.max_retries),
                wait=wait_exponential(**wait_kwargs),
                retry=retry_if_exception_type(Exception),
                reraise=True,
            ):
                with attempt:
                    delivered = await self._server.broadcast_text(message)
                    if delivered:
                        return True
                    logger.debug("WebSocketPublisher has no active subscribers; will retry later")
                    return False
        except Exception as e:
            logger.exception(
                "Failed to broadcast message via WebSocket after {} retries: {}",
                self.max_retries,
                e,
            )
        return False

    async def _worker_loop(self) -> None:
        logger.debug("WebSocketPublisher worker started")
        try:
            while True:
                message = await self._next_message()
                if message is None:
                    break
                delivered = await self._send_with_retry(message)
                if delivered:
                    continue
                if self._stop_event.is_set():
                    break
                await self._requeue_front(message)
                await asyncio.sleep(max(self.retry_backoff_ms, 10) / 1000.0)
        except asyncio.CancelledError:
            raise
        finally:
            logger.debug("WebSocketPublisher worker stopped")

    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        payload = {"type": item_type, "id": int(item_id)}
        try:
            if _orjson is not None:
                message = _orjson.dumps(payload).decode("utf-8")
            else:
                message = json.dumps(payload)
        except Exception:
            message = f'{{"type":"{item_type}","id":{int(item_id)}}}'

        self._ensure_worker()
        await self._enqueue(message)

    async def publish_object(self, envelope: EventEnvelope) -> None:
        data = envelope.to_json_bytes().decode("utf-8")

        self._ensure_worker()
        await self._enqueue(data)

    async def close(self) -> None:
        if self._stop_event.is_set():
            if self._worker_task is not None:
                try:
                    await self._worker_task
                except Exception:
                    pass
            try:
                await self._server.stop()
            except Exception:
                pass
            return

        self._stop_event.set()
        self._message_event.set()

        if self._worker_task is not None:
            try:
                await self._worker_task
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            finally:
                self._worker_task = None

        try:
            await self._server.stop()
        except Exception:
            pass


class RedisStreamsPublisher(Publisher):
    """基于 Redis Streams 的对象事件发布器。"""

    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        consumer_config: ConsumerConfig,
    ) -> None:
        self.redis = redis_client
        self.stream_prefix = consumer_config.stream_prefix
        self.maxlen = consumer_config.max_len
        self.timeout_ms = consumer_config.timeout_ms
        self.max_retries = consumer_config.max_retries
        self.retry_backoff_ms = consumer_config.retry_backoff_ms
        self.id_queue_key = consumer_config.id_queue_key

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
            logger.exception("{} after {} retries: {}", fail_log_msg, self.max_retries, e)
            raise

    async def publish_id(self, item_type: ItemType, item_id: int) -> None:
        payload = {"type": item_type, "id": int(item_id)}
        try:
            if _orjson is not None:
                message = _orjson.dumps(payload).decode("utf-8")
            else:
                message = json.dumps(payload)
        except Exception:
            message = f'{{"type":"{item_type}","id":{int(item_id)}}}'

        await self._retry(
            lambda: self.redis.lpush(self.id_queue_key, message),  # type: ignore
            fail_log_msg=f"Failed to enqueue to list={self.id_queue_key}",
        )

        try:
            await self.redis.ltrim(self.id_queue_key, 0, self.maxlen - 1)  # type: ignore
        except Exception:
            pass

    async def publish_object(self, envelope: EventEnvelope) -> None:
        stream = f"{self.stream_prefix}:{envelope.fid}:{envelope.object_type}"
        data = envelope.to_json_bytes()

        entry = {"data": data}

        await self._retry(
            lambda: self.redis.xadd(
                stream,
                cast("Any", entry),
                maxlen=self.maxlen,
            ),
            fail_log_msg=f"Failed to publish to stream={stream}",
        )


def build_envelope(
    item_type: ItemType,
    obj: DTOType,
    *,
    event_type: str = "upsert",
    backfill: bool = False,
) -> EventEnvelope:
    schema = f"tieba.{item_type}.v1"
    if item_type == "thread":
        eid = obj.tid
    else:
        eid = obj.pid
    payload = obj.model_dump(mode="json")

    return EventEnvelope(
        schema=schema,
        type=event_type,
        object_type=item_type,
        fid=obj.fid,
        object_id=eid,
        time=_now_ms(),
        source="scraper",
        backfill=backfill,
        payload=payload,
    )
