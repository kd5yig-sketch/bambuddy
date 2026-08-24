"""Moonraker client for Klipper printer communication.

Moonraker (https://moonraker.readthedocs.io/) is the HTTP/WebSocket JSON-RPC
API server that Klipper-based printers run, and that both Fluidd and Mainsail
are built on top of. Supporting Moonraker therefore covers both UIs' printers
with a single adapter.

This client mirrors BambuMQTTClient's (backend/app/services/bambu_mqtt.py)
public surface — constructor callback kwargs, `state: PrinterState`,
`connect()`/`disconnect()`, and the control methods PrinterManager calls —
so PrinterManager can hold either client type behind the same interface. It
populates the *generic* PrinterState fields (temperatures, progress, state,
layer_num) and leaves all AMS/HMS/nozzle-rack fields at their PrinterState
defaults, since Klipper has no equivalent concepts.

Unlike BambuMQTTClient (which wraps the synchronous, thread-based paho-mqtt
library and so needs its own network thread), this client is built on
aiohttp's native async WebSocket support and runs as a plain asyncio task on
the caller's event loop — no extra thread required. `connect()` must
therefore be called from a coroutine running on the loop that should own the
connection (which is how PrinterManager.connect_printer/test_connection
already call it).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from itertools import count

import aiohttp

from backend.app.services.bambu_mqtt import PrinterState

logger = logging.getLogger(__name__)

DEFAULT_PORT = 7125

# Reason slugs mirroring bambu_mqtt's CONNECT_ERROR_* constants, surfaced via
# `last_connect_error` so the add-printer flow can give a specific reason
# instead of an unqualified failure.
CONNECT_ERROR_REFUSED = "refused"
CONNECT_ERROR_AUTH_REJECTED = "auth_rejected"
CONNECT_ERROR_NOT_KLIPPER = "not_klipper"
CONNECT_ERROR_TIMEOUT = "timeout"

_RECONNECT_DELAY_SECONDS = 5.0
_SUBSCRIBE_OBJECTS = {
    "extruder": None,
    "heater_bed": None,
    "print_stats": None,
    "display_status": None,
    "virtual_sdcard": None,
    "toolhead": None,
}

# print_stats.state -> Bambu-vocabulary gcode_state, so downstream code that
# switches on `state.state` (get_derived_status_name, the frontend's status
# derivation) sees a familiar value instead of a third vocabulary.
_KLIPPER_STATE_MAP = {
    "standby": "IDLE",
    "printing": "RUNNING",
    "paused": "PAUSE",
    "complete": "FINISH",
    "cancelled": "FAILED",
    "error": "FAILED",
}


class MoonrakerClient:
    """WebSocket JSON-RPC client for a Klipper printer's Moonraker instance."""

    def __init__(
        self,
        ip_address: str,
        port: int | None = None,
        api_key: str = "",
        model: str | None = None,
        on_state_change: Callable[[PrinterState], None] | None = None,
        on_print_start: Callable[[dict], None] | None = None,
        on_print_complete: Callable[[dict], None] | None = None,
        on_ams_change: Callable[[list], None] | None = None,
        on_layer_change: Callable[[int], None] | None = None,
        on_print_progress: Callable[[int], None] | None = None,
        on_bed_temp_update: Callable[[float], None] | None = None,
        on_drying_complete: Callable[[int], None] | None = None,
        on_print_running_observed: Callable[[dict], None] | None = None,
        on_finish_photo_moment: Callable[[dict], None] | None = None,
        on_assignment_verified: Callable[[int, int, bool, dict], None] | None = None,
        on_tray_change: Callable[[int, int], None] | None = None,
    ):
        self.ip_address = ip_address
        self.port = port or DEFAULT_PORT
        self.api_key = api_key or ""
        self.model = model

        # AMS/HMS-only callbacks are accepted for constructor-compatibility
        # with BambuMQTTClient but never fired — Klipper has no equivalent.
        self.on_state_change = on_state_change
        self.on_print_start = on_print_start
        self.on_print_complete = on_print_complete
        self.on_print_progress = on_print_progress
        self.on_bed_temp_update = on_bed_temp_update

        self.state = PrinterState()
        self.last_connect_error: str | None = None
        self.last_connect_error_name: str | None = None

        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._request_ids = count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._was_printing = False
        self._last_message_at: float = 0.0

    # -- connection lifecycle -------------------------------------------------

    def connect(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Start the connection task on the given (or current running) loop.

        Returns immediately, matching BambuMQTTClient.connect()'s
        fire-and-forget semantics — PrinterManager.connect_printer polls
        `state.connected` after a short sleep rather than awaiting this.
        """
        target_loop = loop or asyncio.get_running_loop()
        self._stopping = False
        self._task = target_loop.create_task(self._run_forever())

    def disconnect(self, timeout: float = 0) -> None:
        """Stop the connection task. Safe to call from sync context."""
        self._stopping = True
        self.state.connected = False
        if self._task and not self._task.done():
            self._task.cancel()

    def is_stale(self, max_age_seconds: float = 60.0) -> bool:
        if not self.state.connected:
            return False
        return self._last_message_at > 0 and (time.monotonic() - self._last_message_at) > max_age_seconds

    def check_staleness(self) -> None:
        if self.is_stale():
            self.state.connected = False
            self._fire_state_change()

    def force_reconnect_stale_session(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = asyncio.get_running_loop().create_task(self._run_forever())

    def mark_power_off(self) -> bool:
        """Presume the printer lost power (smart plug switched off).

        Simpler than BambuMQTTClient.mark_power_off's version: no self-healing
        undo-if-it-keeps-talking logic, since the WebSocket connection itself
        already proves liveness — if the printer is actually still powered,
        the reconnect loop's next successful handshake sets `connected` back
        to True and pushes a fresh status on its own.
        """
        if not self.state.connected:
            return False
        self.state.connected = False
        self.state.state = "unknown"
        return True

    # -- background connection loop -------------------------------------------

    async def _run_forever(self) -> None:
        while not self._stopping:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect loop must not die
                logger.warning("Moonraker connection to %s:%s dropped: %s", self.ip_address, self.port, exc)
                self.last_connect_error = self._classify_error(exc)
            self.state.connected = False
            self._fire_state_change()
            if self._stopping:
                break
            await asyncio.sleep(_RECONNECT_DELAY_SECONDS)

    async def _connect_and_listen(self) -> None:
        # The TCP-connect/handshake timeout belongs on the session, not on
        # ws_connect()'s own `timeout` kwarg — aiohttp >=3.10 repurposed that
        # kwarg to take a ClientWSTimeout (ws_receive/ws_close) rather than a
        # plain float, so passing a number there raises a TypeError.
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10, sock_connect=10))
        try:
            headers = {}
            if self.api_key:
                headers["X-Api-Key"] = self.api_key
            ws_url = f"ws://{self.ip_address}:{self.port}/websocket"
            async with self._session.ws_connect(ws_url, headers=headers, heartbeat=30) as ws:
                self._ws = ws
                await self._subscribe()
                self.state.connected = True
                self.last_connect_error = None
                self._last_message_at = time.monotonic()
                self._fire_state_change()

                async for msg in ws:
                    if self._stopping:
                        break
                    self._last_message_at = time.monotonic()
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle_message(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSING):
                        break
        finally:
            self._ws = None
            await self._session.close()
            self._session = None

    @staticmethod
    def _classify_error(exc: Exception) -> str:
        if isinstance(exc, aiohttp.WSServerHandshakeError):
            if exc.status in (401, 403):
                return CONNECT_ERROR_AUTH_REJECTED
            return CONNECT_ERROR_NOT_KLIPPER
        if isinstance(exc, TimeoutError | asyncio.TimeoutError):
            return CONNECT_ERROR_TIMEOUT
        return CONNECT_ERROR_REFUSED

    # -- JSON-RPC ---------------------------------------------------------------

    async def _send_rpc(self, method: str, params: dict | None = None) -> None:
        """Fire a JSON-RPC request without waiting for the response.

        Mirrors BambuMQTTClient.send_command's fire-and-forget MQTT publish —
        callers don't await command methods, they just trigger the action and
        rely on the subsequent status push to reflect the new state.
        """
        if not self._ws or self._ws.closed:
            logger.debug("Moonraker command %s dropped: not connected", method)
            return
        request_id = next(self._request_ids)
        payload = {"jsonrpc": "2.0", "method": method, "id": request_id}
        if params is not None:
            payload["params"] = params
        await self._ws.send_str(json.dumps(payload))

    def _fire_and_forget(self, coro) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("Moonraker command dropped: no running event loop")
            coro.close()
            return
        task = loop.create_task(coro)
        task.add_done_callback(self._log_task_exception)

    @staticmethod
    def _log_task_exception(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("Moonraker command failed: %s", exc, exc_info=exc)

    async def _subscribe(self) -> None:
        await self._send_rpc("printer.objects.subscribe", {"objects": _SUBSCRIBE_OBJECTS})

    def _handle_message(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        self.state.raw_data = data
        method = data.get("method")
        if method == "notify_status_update":
            params = data.get("params") or [{}]
            self._apply_status(params[0] if params else {})
        elif method == "notify_klippy_disconnected":
            self.state.connected = False
            self._fire_state_change()
        # "result" responses to our own requests (e.g. the initial subscribe
        # reply, which includes the full current status) also carry status.
        elif "result" in data and isinstance(data["result"], dict) and "status" in data["result"]:
            self._apply_status(data["result"]["status"])

    def _apply_status(self, status: dict) -> None:
        s = self.state

        extruder = status.get("extruder")
        if extruder:
            temps = dict(s.temperatures)
            temps["nozzle"] = extruder.get("temperature", temps.get("nozzle"))
            temps["nozzle_target"] = extruder.get("target", temps.get("nozzle_target"))
            s.temperatures = temps

        heater_bed = status.get("heater_bed")
        if heater_bed:
            temps = dict(s.temperatures)
            temps["bed"] = heater_bed.get("temperature", temps.get("bed"))
            temps["bed_target"] = heater_bed.get("target", temps.get("bed_target"))
            s.temperatures = temps
            if self.on_bed_temp_update and "temperature" in heater_bed:
                self.on_bed_temp_update(heater_bed["temperature"])

        virtual_sdcard = status.get("virtual_sdcard")
        if virtual_sdcard and "progress" in virtual_sdcard:
            new_progress = round(virtual_sdcard["progress"] * 100, 1)
            if new_progress != s.progress and self.on_print_progress:
                self.on_print_progress(int(new_progress))
            s.progress = new_progress

        print_stats = status.get("print_stats")
        if print_stats:
            klipper_state = print_stats.get("state")
            if klipper_state:
                s.state = _KLIPPER_STATE_MAP.get(klipper_state, klipper_state.upper())
            if "filename" in print_stats:
                s.gcode_file = print_stats["filename"] or None
                s.subtask_name = s.gcode_file
            info = print_stats.get("info") or {}
            if "current_layer" in info and info["current_layer"] is not None:
                s.layer_num = info["current_layer"]
            if "total_layer" in info and info["total_layer"] is not None:
                s.total_layers = info["total_layer"]

            is_printing = print_stats.get("state") == "printing"
            if is_printing and not self._was_printing and self.on_print_start:
                self.on_print_start({"filename": s.gcode_file, "subtask_name": s.subtask_name, "raw_data": status})
            if self._was_printing and print_stats.get("state") == "complete" and self.on_print_complete:
                self.on_print_complete({"filename": s.gcode_file, "raw_data": status})
            self._was_printing = is_printing

        self._fire_state_change()

    def _fire_state_change(self) -> None:
        if self.on_state_change:
            self.on_state_change(self.state)

    # -- control commands ---------------------------------------------------
    #
    # Signatures and bool return values (True = command dispatched, False =
    # not connected / unsupported) match BambuMQTTClient's exactly — routes
    # like POST /{id}/print/stop do `if not client.stop_print(): raise 500`,
    # so a mismatched signature or an always-None return breaks control
    # end-to-end even though "the message gets sent".

    def _dispatch(self, coro) -> bool:
        if not self.state.connected or not self._ws or self._ws.closed:
            logger.warning("Moonraker command dropped: not connected to %s", self.ip_address)
            return False
        self._fire_and_forget(coro)
        return True

    def send_gcode(self, gcode: str) -> bool:
        return self._dispatch(self._send_rpc("printer.gcode.script", {"script": gcode}))

    def start_print(self, filename: str, *_args, **_kwargs) -> bool:
        """Start printing a file already uploaded to the gcodes root.

        Accepts and ignores BambuMQTTClient.start_print's AMS/calibration
        kwargs (plate_id, ams_mapping, bed_levelling, ...) — PrinterManager
        passes them through unconditionally from print_scheduler.py so that
        call site doesn't need a protocol branch; Klipper has no equivalent
        concepts for any of them.
        """
        return self._dispatch(self._send_rpc("printer.print.start", {"filename": filename.lstrip("/")}))

    def pause_print(self) -> bool:
        return self._dispatch(self._send_rpc("printer.print.pause"))

    def resume_print(self) -> bool:
        return self._dispatch(self._send_rpc("printer.print.resume"))

    def stop_print(self) -> bool:
        return self._dispatch(self._send_rpc("printer.print.cancel"))

    def request_status_update(self) -> bool:
        return self._dispatch(self._subscribe())

    def set_bed_temperature(self, target: int) -> bool:
        return self.send_gcode(f"M140 S{target}")

    def set_nozzle_temperature(self, target: int, nozzle: int = 0) -> bool:
        return self.send_gcode(f"M104 T{nozzle} S{target}")

    def set_fan_speed(self, fan: int, speed: int) -> bool:
        """Set a fan's PWM speed (0-255, already-scaled — matches BambuMQTTClient).

        Klipper's plain ``M106`` only addresses the default part-cooling fan
        (fan index 1); auxiliary/chamber fans are user-named in the printer's
        own config with no standard index, so those aren't dispatchable
        generically here and return False rather than guessing a gcode
        target that may not exist.
        """
        if fan != 1:
            logger.info("Moonraker set_fan_speed: fan index %s has no standard Klipper mapping, ignoring", fan)
            return False
        return self.send_gcode(f"M106 S{max(0, min(255, speed))}")
