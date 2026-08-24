"""Mock Moonraker WebSocket JSON-RPC server for testing MoonrakerClient.

Built on aiohttp (same library MoonrakerClient itself uses), mirroring the
"real mock server, not a fully-mocked client" philosophy of mock_ftp_server.py
— this exercises the actual wire protocol (JSON-RPC framing, headers,
notifications) rather than stubbing out MoonrakerClient's internals.
"""

from __future__ import annotations

import asyncio
import json

from aiohttp import WSMsgType, web


class MockMoonrakerServer:
    """A minimal Moonraker instance: /websocket JSON-RPC + status notifications."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.received_calls: list[dict] = []  # {"method": ..., "params": ...}
        self.subscribed_sockets: list[web.WebSocketResponse] = []
        self._status: dict = {
            "extruder": {"temperature": 25.0, "target": 0.0},
            "heater_bed": {"temperature": 24.0, "target": 0.0},
            "print_stats": {"state": "standby", "filename": "", "info": {}},
            "virtual_sdcard": {"progress": 0.0},
            "toolhead": {},
        }
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.port: int | None = None
        self.reject_handshake_status: int | None = None  # e.g. 401 to simulate auth failure

    async def start(self, port: int = 0) -> int:
        app = web.Application()
        app.router.add_get("/websocket", self._handle_ws)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", port)
        await self._site.start()
        # aiohttp doesn't expose the bound port directly on TCPSite; pull it
        # from the underlying server socket.
        self.port = self._site._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        for ws in list(self.subscribed_sockets):
            if not ws.closed:
                await ws.close()
        if self._runner:
            await self._runner.cleanup()

    async def _handle_ws(self, request: web.Request) -> web.StreamResponse:
        if self.reject_handshake_status:
            return web.Response(status=self.reject_handshake_status)
        if self.api_key and request.headers.get("X-Api-Key") != self.api_key:
            return web.Response(status=401)

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.subscribed_sockets.append(ws)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                method = data.get("method")
                req_id = data.get("id")
                params = data.get("params")
                self.received_calls.append({"method": method, "params": params})

                if method == "printer.objects.subscribe":
                    await ws.send_str(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": {"status": self._status}}))
                elif method is not None:
                    await ws.send_str(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": "ok"}))
        finally:
            if ws in self.subscribed_sockets:
                self.subscribed_sockets.remove(ws)
        return ws

    async def push_status_update(self, partial_status: dict) -> None:
        """Merge into current status and notify all connected clients, like a real printer push."""
        for key, value in partial_status.items():
            self._status.setdefault(key, {})
            if isinstance(value, dict):
                self._status[key].update(value)
            else:
                self._status[key] = value
        notification = json.dumps({"jsonrpc": "2.0", "method": "notify_status_update", "params": [partial_status, 0.0]})
        for ws in list(self.subscribed_sockets):
            if not ws.closed:
                await ws.send_str(notification)

    def calls_for(self, method: str) -> list[dict]:
        return [c for c in self.received_calls if c["method"] == method]

    async def disconnect_all(self) -> None:
        """Simulate the printer dropping the connection, to test client reconnect."""
        for ws in list(self.subscribed_sockets):
            await ws.close()
        await asyncio.sleep(0)
