"""事件发布模块（对象直推）。

提供统一的发布接口与 Redis Streams 实现，用于将 Thread/Post/Comment 的对象事件
以 JSON 形式写入 Redis Streams。支持重试与配置化的 stream 前缀。
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, cast
from zoneinfo import ZoneInfo

from aiohttp import web
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
    object_type: str
    fid: int
    object_id: int
    time: int
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

    启动一个内置的 WebSocket 服务端，接受下游消费者连接，并向所有在线连接广播事件。
    """

    def __init__(
        self,
        url: str,
        *,
        timeout_ms: int = 2000,
        max_retries: int = 5,
        retry_backoff_ms: int = 200,
        json_compact: bool = True,
    ) -> None:
        from urllib.parse import urlparse

        self.url = url
        self.timeout_ms = timeout_ms
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.json_compact = json_compact

        p = urlparse(url)
        self._host = p.hostname or "127.0.0.1"
        self._port = p.port or 8000
        self._path = p.path or "/ws"

        self._server_started = False
        self._server_lock = asyncio.Lock()
        # aiohttp 相关对象
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._conns: set[web.WebSocketResponse] = set()
        self._bound_port: int | None = None

    async def _ws_handler(self, request: Any) -> web.WebSocketResponse:
        """aiohttp WebSocket 处理器，按固定 path 建立连接并纳入广播集合。"""

        ws = web.WebSocketResponse(autoclose=True, autoping=True)
        await ws.prepare(request)

        self._conns.add(ws)
        try:
            # 读取并忽略客户端消息，仅用于保持连接直到对端关闭
            async for _ in ws:
                pass
        finally:
            self._conns.discard(ws)
        return ws

    async def _ensure_server(self) -> None:
        if self._server_started:
            return
        async with self._server_lock:
            if self._server_started:
                return

            self._app = web.Application()
            # 仅注册指定 path 的 WebSocket 路由，其它路径返回 404
            self._app.add_routes([web.get(self._path, self._ws_handler)])

            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()

            # 若端口为 0，记录实际绑定端口用于测试/观测
            actual_port = self._port
            try:
                addresses = getattr(self._runner, "addresses", None)
                if addresses:
                    addr0 = addresses[0]
                    if isinstance(addr0, tuple) and len(addr0) >= 2:
                        actual_port = int(addr0[1])
                    elif isinstance(addr0, dict) and "port" in addr0:
                        actual_port = int(addr0["port"])
                else:
                    sockets = getattr(getattr(self._site, "_server", None), "sockets", None)
                    if sockets:
                        actual_port = int(sockets[0].getsockname()[1])
            except Exception:
                pass
            self._bound_port = actual_port

            self._server_started = True
            log.info(f"WebSocketPublisher serving on ws://{self._host}:{self._port}{self._path}")

    def get_ws_url(self) -> str:
        """返回当前服务端可连接的 WebSocket URL（包含实际绑定端口）。"""
        port = self._bound_port or self._port
        return f"ws://{self._host}:{port}{self._path}"

    async def _broadcast_text(self, text: str) -> None:
        await self._ensure_server()
        if not self._conns:
            # 无消费者连接时直接返回
            return
        to_remove: list[web.WebSocketResponse] = []
        for ws in list(self._conns):
            try:
                await ws.send_str(text)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            self._conns.discard(ws)

    async def close(self) -> None:
        """优雅关闭：断开所有连接并停止站点/runner。"""
        # 关闭所有客户端连接
        for ws in list(self._conns):
            try:
                await ws.close()
            except Exception:
                pass
        self._conns.clear()

        # 停止站点和清理 runner
        try:
            if self._site is not None:
                await self._site.stop()
        except Exception:
            pass
        try:
            if self._runner is not None:
                await self._runner.cleanup()
        except Exception:
            pass

        self._app = None
        self._runner = None
        self._site = None
        self._server_started = False

    async def _retry(self, func: Callable[[], Awaitable[Any]], *, fail_log_msg: str) -> Any:
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.max_retries),
                wait=wait_fixed(max(self.retry_backoff_ms, 0) / 1000.0),
                retry=retry_if_exception_type(Exception),
                reraise=True,
            ):
                with attempt:
                    return await func()
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
            lambda: self._broadcast_text(message),
            fail_log_msg="Failed to broadcast ID message via WebSocket",
        )

    async def publish_object(self, envelope: EventEnvelope) -> None:
        data = envelope.to_json_bytes(self.json_compact).decode("utf-8")

        await self._retry(
            lambda: self._broadcast_text(data),
            fail_log_msg="Failed to broadcast object via WebSocket",
        )


class RedisStreamsPublisher(Publisher):
    """基于 Redis Streams 的对象事件发布器。"""

    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        stream_prefix: str = "scraper:tieba:events",
        maxlen: int = 10000,
        approx: bool = True,
        json_compact: bool = True,
        timeout_ms: int = 2000,
        max_retries: int = 5,
        retry_backoff_ms: int = 200,
        id_queue_key: str = "scraper:tieba:queue",
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

    async def publish_object(self, envelope: EventEnvelope) -> None:
        stream = f"{self.stream_prefix}:{envelope.fid}:{envelope.object_type}"
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
        type=event_type,
        object_type=item_type,
        fid=obj.fid,
        object_id=eid,
        time=_now_ms(),
        source="scraper",
        backfill=backfill,
        payload=payload,
    )
