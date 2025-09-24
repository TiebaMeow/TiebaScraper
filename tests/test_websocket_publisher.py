import json

import pytest
from aiohttp import ClientSession, WSMsgType

from src.core.publisher import EventEnvelope, WebSocketPublisher


@pytest.mark.asyncio
async def test_websocket_publisher_broadcast_and_close():
    # 使用端口 0 让系统自动分配空闲端口，避免冲突
    pub = WebSocketPublisher("ws://127.0.0.1:0/ws")

    # 确保服务启动
    await pub._ensure_server()  # type: ignore[attr-defined]
    url = pub.get_ws_url()

    # 建立客户端连接
    async with ClientSession() as session:
        async with session.ws_connect(url) as ws:
            # 广播 ID 消息
            await pub.publish_id("thread", 123)
            msg = await ws.receive(timeout=2.0)
            assert msg.type == WSMsgType.TEXT
            data = json.loads(msg.data)
            assert data == {"type": "thread", "id": 123}

            # 广播对象事件
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

    # 关闭 publisher，应不抛错
    await pub.close()

    # 再次 close 也应安全（幂等）
    await pub.close()
