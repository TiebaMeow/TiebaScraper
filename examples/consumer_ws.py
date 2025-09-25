import asyncio
import json
from typing import Any, Literal, cast

from aiohttp import ClientSession, WSMsgType
from deserialization import deserialize

WS_URL = "ws://127.0.0.1:8000/ws"


async def handle_message(raw: str) -> None:
    """示例：处理从 WebSocket 接收到的消息。"""
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


async def _send_forum_command(
    command: Literal["add_forum", "remove_forum"],
    fname: str,
    *,
    url: str = WS_URL,
    token: str | None = None,
) -> None:
    """示例：向 WebSocket 服务器发送 add/remove_forum 控制命令。"""

    payload = json.dumps({"type": command, "fname": fname})
    params: dict[str, str] | None = {"token": token} if token else None

    try:
        async with ClientSession() as session:
            async with session.ws_connect(url, params=params) as ws:
                await ws.send_str(payload)
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=3.0)
                except TimeoutError:
                    print(f"[warn] no {command} ack for {fname!r}")
                    return

                if msg.type == WSMsgType.TEXT:
                    print(f"[info] {command} ack: {msg.data}")
                elif msg.type == WSMsgType.ERROR:
                    print(f"[error] {command} failed: {getattr(msg, 'data', None)}")
                else:
                    print(f"[warn] unexpected response: {msg.type}")
    except Exception as exc:
        print(f"[error] {command} command error for {fname!r}: {exc}")


async def add_forum_example(fname: str, *, url: str = WS_URL, token: str | None = None) -> None:
    """示例：请求 Scraper 添加监控贴吧。"""

    await _send_forum_command("add_forum", fname, url=url, token=token)


async def remove_forum_example(fname: str, *, url: str = WS_URL, token: str | None = None) -> None:
    """示例：请求 Scraper 取消监控贴吧。"""

    await _send_forum_command("remove_forum", fname, url=url, token=token)


async def main() -> None:
    """示例：通过 WebSocket 消费 Scraper 推送的事件。"""

    url = WS_URL
    # await add_forum_example("吧名", url=url)
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
