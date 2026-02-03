"""内置 WebSocket 服务器。

提供可复用的 WS 服务端能力：
- 可选 Token 验证（query 参数 token= 或 Sec-WebSocket-Protocol 子协议）
- 接受 JSON 命令，当前支持：
  {"type":"add_forum","fname":"吧名"}
- 广播 API：可用于对象事件分发（由 WebSocketPublisher 复用）
- 预留 metrics 推送接口（未来用于主动上报运行指标）

设计要点：
- 单例化服务器实例由上层持有（避免重复绑定端口）；
- 不感知具体业务，只暴露钩子：on_add_forum(callback) 用于接入实际添加流程。
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

from aiohttp import web
from tiebameow.utils.logger import logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pydantic import WebsocketUrl

    ForumHandler = Callable[..., Awaitable[bool]]


class WebSocketServer:
    def __init__(self, url: WebsocketUrl, token: str | None) -> None:
        self._host = url.host or "127.0.0.1"
        self._port = url.port or 8000
        self._path = url.path or "/ws"
        self._token = token
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._server_lock = asyncio.Lock()
        self._started = False
        self._conns: set[web.WebSocketResponse] = set()
        self._add_forum_handler: ForumHandler | None = None
        self._remove_forum_handler: ForumHandler | None = None
        self._bound_port: int | None = None

    def on_add_forum(self, handler: ForumHandler) -> None:
        self._add_forum_handler = handler

    def on_remove_forum(self, handler: ForumHandler) -> None:
        self._remove_forum_handler = handler

    def _auth_passed(self, request: web.Request) -> bool:
        if not self._token:
            return True
        # 支持请求参数和子协议两种方式传递 Token
        qtok = request.query.get("token")
        if qtok and qtok == self._token:
            return True
        proto = request.headers.get("Sec-WebSocket-Protocol", "")
        if proto.startswith("Bearer ") and proto.removeprefix("Bearer ") == self._token:
            return True
        return False

    async def _ws_handler(self, request: web.Request) -> web.StreamResponse:
        if not self._auth_passed(request):
            return web.Response(status=401, text="unauthorized")

        ws = web.WebSocketResponse(autoclose=True, autoping=True)
        await ws.prepare(request)
        self._conns.add(ws)
        try:
            async for msg in ws:  # 接收控制命令
                if msg.type != web.WSMsgType.TEXT:
                    continue
                try:
                    payload = json.loads(msg.data)
                except Exception:
                    await ws.send_str(json.dumps({"ok": False, "error": "invalid_json"}))
                    continue

                mtype = payload.get("type")
                if mtype == "ping":
                    await ws.send_str(json.dumps({"type": "pong"}))
                    continue

                if mtype == "add_forum" or mtype == "remove_forum":
                    fname = str(payload.get("fname", "")).strip()
                    group = payload.get("group")
                    if not fname:
                        await ws.send_str(json.dumps({"ok": False, "error": "fname_required"}))
                        continue
                    try:
                        if mtype == "add_forum":
                            if not self._add_forum_handler:
                                await ws.send_str(json.dumps({"ok": False, "error": "handler_not_ready"}))
                                continue
                            ok = await self._add_forum_handler(fname, group)
                        else:
                            if not self._remove_forum_handler:
                                await ws.send_str(json.dumps({"ok": False, "error": "handler_not_ready"}))
                                continue
                            ok = await self._remove_forum_handler(fname)
                        await ws.send_str(json.dumps({"ok": bool(ok), "type": f"{mtype}_ack", "fname": fname}))
                    except Exception as e:
                        logger.exception("{} handler failed: {}", mtype, e)
                        await ws.send_str(json.dumps({"ok": False, "error": "internal"}))
                    continue

                await ws.send_str(json.dumps({"ok": False, "error": "unknown_type"}))
        finally:
            self._conns.discard(ws)
        return ws

    async def start(self) -> None:
        if self._started:
            return
        async with self._server_lock:
            if self._started:
                return
            self._app = web.Application()
            self._app.add_routes([web.get(self._path, self._ws_handler)])
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            # 记录实际绑定端口（支持端口为 0 的场景）
            self._bound_port = None
            try:
                if self._runner is not None:
                    addrs = self._runner.addresses
                    if addrs:
                        # 取第一个地址的端口
                        self._bound_port = int(addrs[0][1])
            except Exception:
                self._bound_port = None
            self._started = True
            logger.info("WS server listening on ws://{}:{}{}", self._host, self._port, self._path)

    async def stop(self) -> None:
        for ws in list(self._conns):
            try:
                await ws.close()
            except Exception:
                pass
        self._conns.clear()

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
        self._started = False

    async def broadcast_text(self, text: str) -> bool:
        if not self._started:
            await self.start()
        if not self._conns:
            return False
        to_remove: list[web.WebSocketResponse] = []
        delivered = False
        for ws in list(self._conns):
            try:
                await ws.send_str(text)
                delivered = True
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            self._conns.discard(ws)
        return delivered

    # 预留：主动推送指标（未来可定时调用）
    async def push_metrics(self, data: dict[str, Any]) -> bool:
        payload = json.dumps({"type": "metrics", "data": data})
        return await self.broadcast_text(payload)

    # 工具方法：返回可连接的 ws:// URL（适配随机端口绑定）
    def get_ws_url(self) -> str:
        port = self._bound_port or self._port
        return f"ws://{self._host}:{port}{self._path}"
