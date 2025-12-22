import asyncio
import json
from typing import cast

import pytest
from aiohttp import ClientSession, WSMsgType
from pydantic import TypeAdapter, WebsocketUrl

from src.core.config import ConsumerConfig
from src.core.publisher import EventEnvelope, WebSocketPublisher
from src.core.ws_server import WebSocketServer

_WS_URL_ADAPTER = TypeAdapter(WebsocketUrl)


def _ws_url(url: str) -> WebsocketUrl:
    return _WS_URL_ADAPTER.validate_python(url)


@pytest.mark.asyncio
async def test_websocket_publisher_queue_worker_and_close():
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    pub = WebSocketPublisher(
        _ws_url(server.get_ws_url()),
        server=server,
        consumer_config=ConsumerConfig(max_len=16, timeout_ms=2000, max_retries=3, retry_backoff_ms=50),
    )

    async with ClientSession() as session:
        async with session.ws_connect(server.get_ws_url()) as ws:
            await pub.publish_id("thread", 123)
            msg = await ws.receive(timeout=2.0)
            assert msg.type == WSMsgType.TEXT
            data = json.loads(msg.data)
            assert data == {"type": "thread", "id": 123}

            env = EventEnvelope(
                schema="tieba.thread.v1",
                type="upsert",
                object_type="thread",
                fid=1,
                object_id=123,
                time=0,
                source="scraper",
                backfill=False,
                payload={"k": "v"},
            )
            await pub.publish_object(env)
            msg2 = await ws.receive(timeout=2.0)
            assert msg2.type == WSMsgType.TEXT
            data2 = json.loads(msg2.data)
            assert data2["object_type"] == "thread"
            assert data2["object_id"] == 123
            assert data2["payload"] == {"k": "v"}

    await pub.close()
    await pub.close()


class _DummyServer:
    def __init__(self, *, fail_times: int = 0, delay: float = 0.0) -> None:
        self.fail_times = fail_times
        self.delay = delay
        self.messages: list[str] = []
        self.broadcast_attempts = 0
        self.stopped = False

    async def broadcast_text(self, text: str) -> bool:
        self.broadcast_attempts += 1
        await asyncio.sleep(self.delay)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("transient")
        self.messages.append(text)
        return True

    async def stop(self) -> None:
        self.stopped = True


@pytest.mark.asyncio
async def test_websocket_publisher_queue_drop_and_retry():
    server = _DummyServer(fail_times=2, delay=0.05)
    pub = WebSocketPublisher(
        _ws_url("ws://dummy"),
        server=cast("WebSocketServer", server),
        consumer_config=ConsumerConfig(max_len=2, timeout_ms=500, max_retries=5, retry_backoff_ms=10),
    )

    try:
        await pub.publish_id("thread", 1)
        await asyncio.sleep(0)  # 让 worker 有机会启动并读取首条消息

        await asyncio.gather(
            pub.publish_id("thread", 2),
            pub.publish_id("thread", 3),
            pub.publish_id("thread", 4),
        )

        await asyncio.sleep(0.6)
    finally:
        await pub.close()

    ids = [json.loads(m)["id"] for m in server.messages]
    assert ids[0] == 1
    assert 2 not in ids
    assert {3, 4}.issubset(set(ids))
    assert server.broadcast_attempts >= len(server.messages) + 2  # 前两次失败的重试
    assert server.stopped is True


@pytest.mark.asyncio
async def test_websocket_publisher_delivers_after_subscriber_reconnects():
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    pub = WebSocketPublisher(
        _ws_url(server.get_ws_url()),
        server=server,
        consumer_config=ConsumerConfig(max_len=8, timeout_ms=1000, max_retries=5, retry_backoff_ms=50),
    )

    try:
        await pub.publish_id("thread", 999)
        await asyncio.sleep(0.15)  # 让 worker 尝试发送并缓存

        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
                msg = await ws.receive(timeout=2.0)
                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data == {"type": "thread", "id": 999}

                await pub.publish_id("thread", 1000)
                msg2 = await ws.receive(timeout=2.0)
                assert msg2.type == WSMsgType.TEXT
                data2 = json.loads(msg2.data)
                assert data2 == {"type": "thread", "id": 1000}
    finally:
        await pub.close()
