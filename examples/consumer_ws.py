import asyncio
import json
from typing import Any, Literal, cast

from aiohttp import ClientSession, WSMsgType
from deserialization import deserialize

WS_URL = "ws://127.0.0.1:8000/ws"


async def handle_message(raw: str) -> None:
    try:
        data: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[warn] non-json message: {raw!r}")
        return

    # 判定两类负载：ID 消息或对象事件（EventEnvelope）
    if "schema" in data and "payload" in data:
        object_type = data.get("object_type")
        object_id = data.get("object_id")
        payload = data.get("payload")
        print(f"[info] object event: {object_type} id={object_id}")
        try:
            # 约束类型，保证静态检查友好
            itype = cast("Literal['thread','post','comment']", object_type)
            pdata = cast("dict[str, Any]", payload)
            obj = deserialize(itype, pdata)
            # 在此处理接受到的对象
            print(obj)
        except Exception as e:
            print(f"[error] deserialize failed: {e}")
    elif "type" in data and "id" in data:
        print(f"[info] id message: type={data['type']} id={data['id']}")
    else:
        print(f"[warn] unknown message shape: {data}")


async def main() -> None:
    """示例：通过 WebSocket 消费 Scraper 推送的事件。"""

    url = WS_URL
    try:
        async with ClientSession() as session:
            async with session.ws_connect(url) as ws:
                print(f"[info] connected to {url}")
                while True:
                    try:
                        msg = await ws.receive()
                    except Exception as e:
                        print(f"[error] ws.receive() failed: {e}")
                        await asyncio.sleep(1)
                        continue

                    if msg.type == WSMsgType.TEXT:
                        await handle_message(cast("str", msg.data))
                    elif msg.type == WSMsgType.BINARY:
                        text = cast("bytes", msg.data).decode("utf-8", errors="replace")
                        await handle_message(text)
                    elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSING, WSMsgType.CLOSE):
                        print("[info] server closed connection")
                        break
                    elif msg.type == WSMsgType.ERROR:
                        print(f"[error] ws error: {getattr(msg, 'data', None)}")
                        break
                    else:
                        # PING/PONG/其他信令
                        await asyncio.sleep(0)
    except KeyboardInterrupt:
        print("[info] shutting down consumer...")


if __name__ == "__main__":
    asyncio.run(main())
