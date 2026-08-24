"""Integration-style tests for MoonrakerClient against a real mock Moonraker server.

Mirrors test_bambu_ftp.py's philosophy: exercise the actual wire protocol
(JSON-RPC over a real aiohttp WebSocket connection) rather than mocking
MoonrakerClient's internals, so a wrong method name, a wrong param shape, or
an aiohttp API mismatch (see the ws_connect `timeout` kwarg fix this test
suite caught) shows up as a real failure.
"""

import asyncio
import socket

import pytest

from backend.app.services.moonraker_client import (
    CONNECT_ERROR_AUTH_REJECTED,
    MoonrakerClient,
)
from backend.tests.unit.services.mock_moonraker_server import MockMoonrakerServer


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.05) -> bool:
    """Poll `predicate()` until it's truthy or `timeout` elapses."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


@pytest.fixture
async def moonraker_server():
    server = MockMoonrakerServer()
    await server.start(port=_find_free_port())
    yield server
    await server.stop()


@pytest.fixture
async def connected_client(moonraker_server):
    """A MoonrakerClient already connected to `moonraker_server`."""
    client = MoonrakerClient(ip_address="127.0.0.1", port=moonraker_server.port)
    client.connect()
    connected = await _wait_until(lambda: client.state.connected)
    assert connected, "client did not connect to mock Moonraker server"
    yield client
    client.disconnect()


class TestConnection:
    async def test_connect_success(self, moonraker_server):
        client = MoonrakerClient(ip_address="127.0.0.1", port=moonraker_server.port)
        client.connect()
        assert await _wait_until(lambda: client.state.connected)
        assert client.last_connect_error is None
        client.disconnect()

    async def test_initial_subscribe_populates_state(self, connected_client):
        # mock server's default status: nozzle 25.0C, bed 24.0C, standby.
        assert connected_client.state.temperatures.get("nozzle") == 25.0
        assert connected_client.state.temperatures.get("bed") == 24.0
        assert connected_client.state.state == "IDLE"

    async def test_connect_wrong_api_key_rejected(self):
        server = MockMoonrakerServer(api_key="secret-key")
        await server.start(port=_find_free_port())
        try:
            client = MoonrakerClient(ip_address="127.0.0.1", port=server.port, api_key="wrong-key")
            client.connect()
            # Give it time to attempt + fail the handshake without waiting for a full reconnect cycle.
            await asyncio.sleep(0.5)
            assert client.state.connected is False
            assert client.last_connect_error == CONNECT_ERROR_AUTH_REJECTED
            client.disconnect()
        finally:
            await server.stop()

    async def test_connect_correct_api_key_succeeds(self):
        server = MockMoonrakerServer(api_key="secret-key")
        await server.start(port=_find_free_port())
        try:
            client = MoonrakerClient(ip_address="127.0.0.1", port=server.port, api_key="secret-key")
            client.connect()
            assert await _wait_until(lambda: client.state.connected)
            client.disconnect()
        finally:
            await server.stop()

    async def test_connect_unreachable_host_does_not_raise(self):
        # TEST-NET-1 (RFC 5737) — guaranteed unreachable, connect attempt
        # should fail quietly and leave the reconnect loop running rather
        # than raising out of the background task.
        client = MoonrakerClient(ip_address="192.0.2.1", port=7125)
        client.connect()
        await asyncio.sleep(0.5)
        assert client.state.connected is False
        client.disconnect()

    async def test_disconnect_stops_reconnecting(self, moonraker_server):
        client = MoonrakerClient(ip_address="127.0.0.1", port=moonraker_server.port)
        client.connect()
        await _wait_until(lambda: client.state.connected)
        client.disconnect()
        await asyncio.sleep(0.2)
        assert client._task.cancelled() or client._task.done()


class TestStatusUpdates:
    async def test_temperature_update_notification(self, connected_client, moonraker_server):
        await moonraker_server.push_status_update({"extruder": {"temperature": 210.5, "target": 210.0}})
        assert await _wait_until(lambda: connected_client.state.temperatures.get("nozzle") == 210.5)
        assert connected_client.state.temperatures.get("nozzle_target") == 210.0

    async def test_bed_temp_update_fires_callback(self, moonraker_server):
        seen = []
        client = MoonrakerClient(
            ip_address="127.0.0.1",
            port=moonraker_server.port,
            on_bed_temp_update=lambda t: seen.append(t),
        )
        client.connect()
        await _wait_until(lambda: client.state.connected)
        await moonraker_server.push_status_update({"heater_bed": {"temperature": 60.0, "target": 60.0}})
        # The initial `subscribe` response already fires this callback once
        # with the mock server's default bed temp (24.0) — wait for the
        # pushed value specifically rather than "any callback happened".
        assert await _wait_until(lambda: 60.0 in seen)
        client.disconnect()

    async def test_progress_notification_fires_callback_as_0_to_100(self, moonraker_server):
        seen = []
        client = MoonrakerClient(
            ip_address="127.0.0.1",
            port=moonraker_server.port,
            on_print_progress=lambda p: seen.append(p),
        )
        client.connect()
        await _wait_until(lambda: client.state.connected)
        # Moonraker reports progress as 0.0-1.0; MoonrakerClient must rescale
        # to Bambu's 0-100 convention (printer_manager/frontend both assume it).
        await moonraker_server.push_status_update({"virtual_sdcard": {"progress": 0.42}})
        assert await _wait_until(lambda: connected_client_progress(client) == 42.0)
        assert 42 in seen
        client.disconnect()

    async def test_print_start_and_complete_callbacks(self, moonraker_server):
        started, completed = [], []
        client = MoonrakerClient(
            ip_address="127.0.0.1",
            port=moonraker_server.port,
            on_print_start=lambda data: started.append(data),
            on_print_complete=lambda data: completed.append(data),
        )
        client.connect()
        await _wait_until(lambda: client.state.connected)

        await moonraker_server.push_status_update(
            {"print_stats": {"state": "printing", "filename": "test.gcode", "info": {}}}
        )
        assert await _wait_until(lambda: len(started) == 1)
        assert started[0]["filename"] == "test.gcode"

        await moonraker_server.push_status_update({"print_stats": {"state": "complete", "filename": "test.gcode"}})
        assert await _wait_until(lambda: len(completed) == 1)
        client.disconnect()

    @pytest.mark.parametrize(
        "klipper_state,bambu_state",
        [
            ("standby", "IDLE"),
            ("printing", "RUNNING"),
            ("paused", "PAUSE"),
            ("complete", "FINISH"),
            ("cancelled", "FAILED"),
            ("error", "FAILED"),
        ],
    )
    async def test_klipper_state_maps_to_bambu_vocabulary(
        self, connected_client, moonraker_server, klipper_state, bambu_state
    ):
        await moonraker_server.push_status_update({"print_stats": {"state": klipper_state}})
        assert await _wait_until(lambda: connected_client.state.state == bambu_state)


def connected_client_progress(client) -> float:
    return client.state.progress


class TestCommands:
    async def test_send_gcode_dispatches_and_returns_true(self, connected_client, moonraker_server):
        assert connected_client.send_gcode("G28") is True
        assert await _wait_until(lambda: len(moonraker_server.calls_for("printer.gcode.script")) == 1)
        call = moonraker_server.calls_for("printer.gcode.script")[0]
        assert call["params"] == {"script": "G28"}

    async def test_send_gcode_returns_false_when_not_connected(self, moonraker_server):
        client = MoonrakerClient(ip_address="127.0.0.1", port=moonraker_server.port)
        # Never call connect() — state.connected stays False.
        assert client.send_gcode("G28") is False

    async def test_pause_resume_stop_dispatch_correct_methods(self, connected_client, moonraker_server):
        assert connected_client.pause_print() is True
        assert connected_client.resume_print() is True
        assert connected_client.stop_print() is True
        assert await _wait_until(lambda: len(moonraker_server.calls_for("printer.print.pause")) == 1)
        assert len(moonraker_server.calls_for("printer.print.resume")) == 1
        assert len(moonraker_server.calls_for("printer.print.cancel")) == 1

    async def test_set_bed_temperature_sends_m140(self, connected_client, moonraker_server):
        assert connected_client.set_bed_temperature(60) is True
        assert await _wait_until(lambda: len(moonraker_server.calls_for("printer.gcode.script")) == 1)
        assert moonraker_server.calls_for("printer.gcode.script")[0]["params"]["script"] == "M140 S60"

    async def test_set_nozzle_temperature_sends_m104(self, connected_client, moonraker_server):
        assert connected_client.set_nozzle_temperature(210, nozzle=0) is True
        assert await _wait_until(lambda: len(moonraker_server.calls_for("printer.gcode.script")) == 1)
        assert moonraker_server.calls_for("printer.gcode.script")[0]["params"]["script"] == "M104 T0 S210"

    async def test_set_fan_speed_part_fan_sends_m106(self, connected_client, moonraker_server):
        assert connected_client.set_fan_speed(1, 255) is True
        assert await _wait_until(lambda: len(moonraker_server.calls_for("printer.gcode.script")) == 1)
        assert moonraker_server.calls_for("printer.gcode.script")[0]["params"]["script"] == "M106 S255"

    async def test_set_fan_speed_unsupported_fan_returns_false(self, connected_client, moonraker_server):
        # fan=2 ("aux") has no standard Klipper mapping — see moonraker_client.py.
        assert connected_client.set_fan_speed(2, 128) is False
        await asyncio.sleep(0.2)
        assert len(moonraker_server.calls_for("printer.gcode.script")) == 0

    async def test_request_status_update_resubscribes(self, connected_client, moonraker_server):
        before = len(moonraker_server.calls_for("printer.objects.subscribe"))
        assert connected_client.request_status_update() is True
        assert await _wait_until(lambda: len(moonraker_server.calls_for("printer.objects.subscribe")) > before)


class TestReconnect:
    async def test_reconnects_after_server_drops_connection(self, moonraker_server, monkeypatch):
        import backend.app.services.moonraker_client as moonraker_client_module

        monkeypatch.setattr(moonraker_client_module, "_RECONNECT_DELAY_SECONDS", 0.2)

        client = MoonrakerClient(ip_address="127.0.0.1", port=moonraker_server.port)
        client.connect()
        assert await _wait_until(lambda: client.state.connected)

        await moonraker_server.disconnect_all()
        assert await _wait_until(lambda: client.state.connected is False)

        # Reconnect loop should pick the connection back up automatically.
        assert await _wait_until(lambda: client.state.connected, timeout=3.0)
        client.disconnect()


class TestMarkPowerOff:
    def test_mark_power_off_on_connected_client(self, connected_client):
        assert connected_client.mark_power_off() is True
        assert connected_client.state.connected is False
        assert connected_client.state.state == "unknown"

    def test_mark_power_off_on_disconnected_client_returns_false(self, moonraker_server):
        client = MoonrakerClient(ip_address="127.0.0.1", port=moonraker_server.port)
        assert client.mark_power_off() is False


class TestPrinterManagerIntegration:
    """Regression coverage for a real incident: PrinterManager.get_drying_targets
    accessed BambuMQTTClient's private `_drying_targets` cache directly, which
    crashed the websocket status loop for every connected Klipper printer
    (AttributeError on every push — MoonrakerClient has no such attribute).
    All of PrinterManager's own tests mock the client (MagicMock auto-creates
    any attribute you touch, so it can never catch a missing-attribute bug),
    so this exercises a real connected MoonrakerClient instead.
    """

    async def test_get_drying_targets_returns_none_for_klipper_client(self, connected_client):
        from backend.app.services.printer_manager import PrinterManager

        manager = PrinterManager()
        manager._clients[1] = connected_client
        assert manager.get_drying_targets(1) is None
