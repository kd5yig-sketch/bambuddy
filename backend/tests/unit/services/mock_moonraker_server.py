"""Mock Moonraker WebSocket JSON-RPC server for testing MoonrakerClient.

Built on aiohttp (same library MoonrakerClient itself uses), mirroring the
"real mock server, not a fully-mocked client" philosophy of mock_ftp_server.py
— this exercises the actual wire protocol (JSON-RPC framing, headers,
notifications) rather than stubbing out MoonrakerClient's internals.
"""

from __future__ import annotations

import asyncio
import json
import time

from aiohttp import WSMsgType, web


class MockMoonrakerServer:
    """A minimal Moonraker instance: /websocket JSON-RPC + status notifications,
    plus the /server/files/* HTTP API for testing moonraker_files.py."""

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

        # In-memory gcodes root: {filename: bytes}. Mirrors mock_ftp_server's
        # real-filesystem-backed approach closely enough for HTTP semantics.
        self.files: dict[str, bytes] = {}
        self.fail_upload = False  # force the next upload(s) to fail, for error-path tests
        self.disk_usage = {"total": 1_000_000_000, "used": 250_000_000, "free": 750_000_000}

    async def start(self, port: int = 0) -> int:
        app = web.Application()
        app.router.add_get("/websocket", self._handle_ws)
        app.router.add_get("/server/files/list", self._handle_list_files)
        app.router.add_post("/server/files/upload", self._handle_upload)
        app.router.add_get("/server/files/gcodes/{filename}", self._handle_download)
        app.router.add_delete("/server/files/gcodes/{filename}", self._handle_delete)
        app.router.add_get("/server/files/directory", self._handle_directory)
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

    def _check_auth(self, request: web.Request) -> web.Response | None:
        if self.api_key and request.headers.get("X-Api-Key") != self.api_key:
            return web.Response(status=401)
        return None

    async def _handle_list_files(self, request: web.Request) -> web.Response:
        if (rejected := self._check_auth(request)) is not None:
            return rejected
        return web.json_response(
            {
                "result": [
                    {"path": name, "size": len(data), "modified": time.time()} for name, data in self.files.items()
                ]
            }
        )

    async def _handle_upload(self, request: web.Request) -> web.Response:
        if (rejected := self._check_auth(request)) is not None:
            return rejected
        if self.fail_upload:
            return web.Response(status=500, text="simulated upload failure")

        reader = await request.multipart()
        filename = None
        content = b""
        async for part in reader:
            if part.name == "file":
                filename = part.filename
                content = await part.read(decode=False)
            else:
                await part.read()  # drain the "root" field etc.

        if filename is None:
            return web.Response(status=400, text="no file part")
        self.files[filename] = content
        return web.json_response({"result": {"item": {"path": filename, "size": len(content)}}}, status=201)

    async def _handle_download(self, request: web.Request) -> web.StreamResponse:
        if (rejected := self._check_auth(request)) is not None:
            return rejected
        filename = request.match_info["filename"]
        if filename not in self.files:
            return web.Response(status=404)
        return web.Response(body=self.files[filename])

    async def _handle_delete(self, request: web.Request) -> web.Response:
        if (rejected := self._check_auth(request)) is not None:
            return rejected
        filename = request.match_info["filename"]
        if filename not in self.files:
            return web.Response(status=404)
        del self.files[filename]
        return web.json_response({"result": filename})

    async def _handle_directory(self, request: web.Request) -> web.Response:
        if (rejected := self._check_auth(request)) is not None:
            return rejected
        return web.json_response({"result": {"dirs": [], "files": [], "disk_usage": self.disk_usage}})

    def calls_for(self, method: str) -> list[dict]:
        return [c for c in self.received_calls if c["method"] == method]

    async def disconnect_all(self) -> None:
        """Simulate the printer dropping the connection, to test client reconnect."""
        for ws in list(self.subscribed_sockets):
            await ws.close()
        await asyncio.sleep(0)
