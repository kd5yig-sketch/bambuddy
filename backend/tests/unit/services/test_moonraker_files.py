"""Integration-style tests for moonraker_files.py against a real mock Moonraker
HTTP file server (same philosophy as test_bambu_ftp.py / test_moonraker_client.py:
exercise the real wire protocol, not a mocked client).
"""

import socket
from pathlib import Path

import pytest

from backend.app.services.bambu_ftp import DeleteResult, UploadCancelled
from backend.app.services.moonraker_files import (
    delete_file_async,
    download_file_async,
    download_file_bytes_async,
    get_storage_info_async,
    list_files_async,
    upload_file_async,
)
from backend.tests.unit.services.mock_moonraker_server import MockMoonrakerServer


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def moonraker_server():
    server = MockMoonrakerServer()
    await server.start(port=_find_free_port())
    yield server
    await server.stop()


@pytest.fixture
def sample_file(tmp_path) -> Path:
    p = tmp_path / "test_print.gcode"
    p.write_bytes(b"; sample gcode\n" * 1000)  # ~15KB, a couple upload chunks
    return p


class TestUpload:
    async def test_upload_success(self, moonraker_server, sample_file):
        result = await upload_file_async("127.0.0.1", "", sample_file, "/test_print.gcode", port=moonraker_server.port)
        assert result is True
        assert moonraker_server.files.get("test_print.gcode") == sample_file.read_bytes()

    async def test_upload_reports_progress(self, moonraker_server, sample_file):
        seen: list[tuple[int, int]] = []
        await upload_file_async(
            "127.0.0.1",
            "",
            sample_file,
            "/test_print.gcode",
            progress_callback=lambda uploaded, total: seen.append((uploaded, total)),
            port=moonraker_server.port,
        )
        assert len(seen) > 0
        total_size = sample_file.stat().st_size
        assert seen[-1] == (total_size, total_size)
        # Monotonically increasing, and every total matches the real file size.
        assert all(u <= total_size and t == total_size for u, t in seen)

    async def test_upload_server_error_returns_false(self, moonraker_server, sample_file):
        moonraker_server.fail_upload = True
        result = await upload_file_async("127.0.0.1", "", sample_file, "/test_print.gcode", port=moonraker_server.port)
        assert result is False

    async def test_upload_with_api_key_succeeds(self, sample_file):
        server = MockMoonrakerServer(api_key="secret")
        await server.start(port=_find_free_port())
        try:
            result = await upload_file_async("127.0.0.1", "secret", sample_file, "/f.gcode", port=server.port)
            assert result is True
        finally:
            await server.stop()

    async def test_upload_wrong_api_key_returns_false(self, sample_file):
        server = MockMoonrakerServer(api_key="secret")
        await server.start(port=_find_free_port())
        try:
            result = await upload_file_async("127.0.0.1", "wrong", sample_file, "/f.gcode", port=server.port)
            assert result is False
        finally:
            await server.stop()

    async def test_upload_deadline_exceeded_raises_upload_cancelled(self, moonraker_server, sample_file):
        with pytest.raises(UploadCancelled):
            await upload_file_async(
                "127.0.0.1",
                "",
                sample_file,
                "/test_print.gcode",
                timeout=0.0001,  # effectively instant
                port=moonraker_server.port,
            )

    async def test_upload_unreachable_host_returns_false(self, sample_file):
        # An unused local port with nothing listening -> connection refused,
        # not a hang or an uncaught exception.
        result = await upload_file_async("127.0.0.1", "", sample_file, "/f.gcode", port=_find_free_port())
        assert result is False


class TestDownload:
    async def test_download_success(self, moonraker_server, tmp_path):
        moonraker_server.files["existing.gcode"] = b"G28\nG1 X10\n"
        local_path = tmp_path / "downloaded.gcode"
        result = await download_file_async("127.0.0.1", "", "/existing.gcode", local_path, port=moonraker_server.port)
        assert result is True
        assert local_path.read_bytes() == b"G28\nG1 X10\n"

    async def test_download_missing_file_returns_false(self, moonraker_server, tmp_path):
        result = await download_file_async(
            "127.0.0.1", "", "/does_not_exist.gcode", tmp_path / "out.gcode", port=moonraker_server.port
        )
        assert result is False


class TestListFiles:
    async def test_list_files_returns_expected_shape(self, moonraker_server):
        moonraker_server.files["a.gcode"] = b"x" * 100
        moonraker_server.files["b.gcode"] = b"y" * 250
        files = await list_files_async("127.0.0.1", "", port=moonraker_server.port)
        by_name = {f["name"]: f for f in files}
        assert set(by_name) == {"a.gcode", "b.gcode"}
        assert by_name["a.gcode"]["size"] == 100
        assert by_name["a.gcode"]["is_directory"] is False
        assert by_name["a.gcode"]["path"] == "/a.gcode"

    async def test_list_files_empty(self, moonraker_server):
        assert await list_files_async("127.0.0.1", "", port=moonraker_server.port) == []


class TestDelete:
    async def test_delete_existing_file(self, moonraker_server):
        moonraker_server.files["to_delete.gcode"] = b"data"
        result = await delete_file_async("127.0.0.1", "", "/to_delete.gcode", port=moonraker_server.port)
        assert result == DeleteResult.DELETED
        assert "to_delete.gcode" not in moonraker_server.files

    async def test_delete_missing_file_returns_not_found(self, moonraker_server):
        result = await delete_file_async("127.0.0.1", "", "/never_existed.gcode", port=moonraker_server.port)
        assert result == DeleteResult.NOT_FOUND

    async def test_delete_unreachable_host_returns_failed(self):
        result = await delete_file_async("127.0.0.1", "", "/f.gcode", port=_find_free_port())
        assert result == DeleteResult.FAILED


class TestDownloadBytes:
    """download_file_bytes_async — used by printers.py's preview/thumbnail/zip routes."""

    async def test_download_bytes_success(self, moonraker_server):
        moonraker_server.files["preview.gcode"] = b"G28\nG1 X10\n"
        data = await download_file_bytes_async("127.0.0.1", "", "/preview.gcode", port=moonraker_server.port)
        assert data == b"G28\nG1 X10\n"

    async def test_download_bytes_missing_file_returns_none(self, moonraker_server):
        data = await download_file_bytes_async("127.0.0.1", "", "/nope.gcode", port=moonraker_server.port)
        assert data is None

    async def test_download_bytes_unreachable_host_returns_none(self):
        data = await download_file_bytes_async("127.0.0.1", "", "/f.gcode", port=_find_free_port())
        assert data is None


class TestStorageInfo:
    async def test_storage_info_returns_expected_shape(self, moonraker_server):
        moonraker_server.disk_usage = {"total": 100, "used": 40, "free": 60}
        info = await get_storage_info_async("127.0.0.1", "", port=moonraker_server.port)
        assert info == {"free_bytes": 60, "used_bytes": 40}

    async def test_storage_info_unreachable_host_returns_none(self):
        info = await get_storage_info_async("127.0.0.1", "", port=_find_free_port())
        assert info is None
