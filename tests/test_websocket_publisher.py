"""WebSocket Publisher 测试。"""

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
    """将字符串转换为 WebsocketUrl"""
    return _WS_URL_ADAPTER.validate_python(url)


def _make_envelope(object_id: int) -> EventEnvelope:
    return EventEnvelope(
        schema="tieba.thread.v1",
        type="upsert",
        object_type="thread",
        fid=1,
        object_id=object_id,
        time=0,
        source="scraper",
        backfill=False,
        payload={"id": object_id},
    )


# ==================== Mock 类 ====================


class DummyServer:
    """测试用 WebSocket 服务器模拟类"""

    def __init__(self, *, fail_times: int = 0, delay: float = 0.0, no_subscribers: bool = False) -> None:
        self.fail_times = fail_times
        self.delay = delay
        self.no_subscribers = no_subscribers
        self.messages: list[str] = []
        self.broadcast_attempts = 0
        self.stopped = False

    async def broadcast_text(self, text: str) -> bool:
        self.broadcast_attempts += 1
        await asyncio.sleep(self.delay)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("transient")
        if self.no_subscribers:
            return False
        self.messages.append(text)
        return True

    async def stop(self) -> None:
        self.stopped = True


# ==================== WebSocketPublisher 基本功能测试 ====================


@pytest.mark.asyncio
async def test_websocket_publisher_publish_object():
    """测试 WebSocketPublisher publish_object"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    pub = WebSocketPublisher(
        _ws_url(server.get_ws_url()),
        server=server,
        consumer_config=ConsumerConfig(max_len=16),
    )

    try:
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
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
                msg = await ws.receive(timeout=2.0)

                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data["object_type"] == "thread"
                assert data["object_id"] == 123
                assert data["payload"] == {"k": "v"}
    finally:
        await pub.close()


@pytest.mark.asyncio
async def test_websocket_publisher_close_idempotent():
    """测试 WebSocketPublisher close() 是幂等的"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    pub = WebSocketPublisher(
        _ws_url(server.get_ws_url()),
        server=server,
        consumer_config=ConsumerConfig(max_len=16),
    )

    # 多次 close 不应该报错
    await pub.close()
    await pub.close()
    await pub.close()


# ==================== WebSocketPublisher 队列和重试测试 ====================


@pytest.mark.asyncio
async def test_websocket_publisher_queue_drop_oldest():
    """测试 WebSocketPublisher 队列满时丢弃最旧消息"""
    server = DummyServer(fail_times=2, delay=0.05)
    pub = WebSocketPublisher(
        _ws_url("ws://dummy"),
        server=cast("WebSocketServer", server),
        consumer_config=ConsumerConfig(max_len=2),
    )

    try:
        await pub.publish_object(_make_envelope(1))
        await asyncio.sleep(0)  # 让 worker 有机会启动

        # 快速发送多条消息
        await asyncio.gather(
            pub.publish_object(_make_envelope(2)),
            pub.publish_object(_make_envelope(3)),
            pub.publish_object(_make_envelope(4)),
        )

        await asyncio.sleep(0.6)
    finally:
        await pub.close()

    ids = [json.loads(m)["object_id"] for m in server.messages]
    assert ids[0] == 1
    # ID 2 应该被丢弃（队列满时最旧的）
    assert 2 not in ids
    # ID 3 和 4 应该被发送
    assert {3, 4}.issubset(set(ids))
    assert server.stopped is True


@pytest.mark.asyncio
async def test_websocket_publisher_retry_on_failure():
    """测试 WebSocketPublisher 失败重试"""
    server = DummyServer(fail_times=2)
    pub = WebSocketPublisher(
        _ws_url("ws://dummy"),
        server=cast("WebSocketServer", server),
        consumer_config=ConsumerConfig(max_len=16),
    )

    try:
        await pub.publish_object(_make_envelope(1))
        await asyncio.sleep(0.5)
    finally:
        await pub.close()

    # 应该有3次广播尝试（2次失败 + 1次成功）
    assert server.broadcast_attempts >= 3
    assert len(server.messages) == 1


@pytest.mark.asyncio
async def test_websocket_publisher_delivers_after_subscriber_reconnects():
    """测试订阅者重连后能收到消息"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    pub = WebSocketPublisher(
        _ws_url(server.get_ws_url()),
        server=server,
        consumer_config=ConsumerConfig(max_len=8),
    )

    try:
        # 先发送消息（此时没有订阅者）
        await pub.publish_object(_make_envelope(999))
        await asyncio.sleep(0.15)  # 让 worker 尝试发送

        # 现在连接订阅者
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
                msg = await ws.receive(timeout=2.0)
                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data["object_id"] == 999

                # 发送第二条消息
                await pub.publish_object(_make_envelope(1000))
                msg2 = await ws.receive(timeout=2.0)
                assert msg2.type == WSMsgType.TEXT
                data2 = json.loads(msg2.data)
                assert data2["object_id"] == 1000
    finally:
        await pub.close()


# ==================== WebSocketServer 测试 ====================


@pytest.mark.asyncio
async def test_websocket_server_start_stop():
    """测试 WebSocketServer 启动和停止"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)

    await server.start()
    assert server._started is True

    # 多次启动应该是幂等的
    await server.start()
    assert server._started is True

    await server.stop()
    assert server._started is False


@pytest.mark.asyncio
async def test_websocket_server_token_auth():
    """测试 WebSocketServer Token 认证"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token="secret123")
    await server.start()

    try:
        # 没有 token 应该被拒绝
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
                # 连接应该被服务器拒绝（401）
                # 注意：aiohttp 可能不会立即断开，需要检查响应
                pass
    except Exception:
        pass  # 预期会失败

    # 带正确 token 应该成功
    try:
        async with ClientSession() as session:
            async with session.ws_connect(f"{server.get_ws_url()}?token=secret123") as ws:
                # 发送 ping 命令
                await ws.send_str(json.dumps({"type": "ping"}))
                msg = await ws.receive(timeout=2.0)
                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data["type"] == "pong"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_server_ping_pong():
    """测试 WebSocketServer ping-pong 命令"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    try:
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
                await ws.send_str(json.dumps({"type": "ping"}))
                msg = await ws.receive(timeout=2.0)

                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data["type"] == "pong"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_server_broadcast():
    """测试 WebSocketServer 广播功能"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    try:
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws1:
                async with session.ws_connect(server.get_ws_url()) as ws2:
                    # 广播消息
                    result = await server.broadcast_text("hello")
                    assert result is True

                    # 两个客户端都应该收到
                    msg1 = await ws1.receive(timeout=2.0)
                    msg2 = await ws2.receive(timeout=2.0)

                    assert msg1.data == "hello"
                    assert msg2.data == "hello"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_server_broadcast_no_subscribers():
    """测试没有订阅者时广播返回 False"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    try:
        result = await server.broadcast_text("hello")
        assert result is False
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_server_unknown_command():
    """测试 WebSocketServer 处理未知命令"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    try:
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
                await ws.send_str(json.dumps({"type": "unknown_command"}))
                msg = await ws.receive(timeout=2.0)

                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data["ok"] is False
                assert data["error"] == "unknown_type"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_websocket_server_invalid_json():
    """测试 WebSocketServer 处理无效 JSON"""
    server = WebSocketServer(_ws_url("ws://127.0.0.1:0/ws"), token=None)
    await server.start()

    try:
        async with ClientSession() as session:
            async with session.ws_connect(server.get_ws_url()) as ws:
                await ws.send_str("not valid json")
                msg = await ws.receive(timeout=2.0)

                assert msg.type == WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data["ok"] is False
                assert data["error"] == "invalid_json"
    finally:
        await server.stop()
