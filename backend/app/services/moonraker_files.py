"""Moonraker file-transfer client for Klipper printers.

Mirrors bambu_ftp.py's async wrapper function names/signatures
(upload_file_async, download_file_async, list_files_async, delete_file_async)
so print_scheduler.py and the printers.py file-browse routes can branch on
printer.protocol and call either module through the same shape, reusing
bambu_ftp's protocol-agnostic UploadCancelled/DeleteResult/with_ftp_retry
rather than duplicating them.

Unlike BambuFTPClient (a synchronous ftplib-based client run in an executor
thread — see bambu_ftp.py's _ftp_executor), this is natively async via
aiohttp's HTTP client, since Moonraker's file API is plain HTTP rather than
FTP. No executor thread is needed.

Moonraker file API reference (server/files):
  GET    /server/files/list?root=gcodes          -> list of {path, size, modified, permissions}
  POST   /server/files/upload  (multipart: file, root=gcodes)
  GET    /server/files/gcodes/{path}              -> raw file bytes
  DELETE /server/files/gcodes/{path}
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

import aiohttp

from backend.app.services.bambu_ftp import DeleteResult, UploadCancelled, _upload_deadline
from backend.app.services.moonraker_client import DEFAULT_PORT

logger = logging.getLogger(__name__)

_UPLOAD_CHUNK_BYTES = 256 * 1024

# One upload at a time per printer, matching bambu_ftp's _upload_lock — two
# concurrent uploads to the same printer would otherwise race on Moonraker's
# gcodes directory the same way they'd corrupt a Bambu SD card transfer.
_upload_locks: dict[str, asyncio.Lock] = {}


def _lock_for(ip_address: str) -> asyncio.Lock:
    lock = _upload_locks.get(ip_address)
    if lock is None:
        lock = asyncio.Lock()
        _upload_locks[ip_address] = lock
    return lock


def _headers(access_code: str) -> dict[str, str]:
    return {"X-Api-Key": access_code} if access_code else {}


def _base_url(ip_address: str, port: int | None) -> str:
    return f"http://{ip_address}:{port or DEFAULT_PORT}"


async def upload_file_async(
    ip_address: str,
    access_code: str,
    local_path: Path,
    remote_path: str,
    timeout: float | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    socket_timeout: float | None = None,  # noqa: ARG001 - accepted for call-site symmetry with bambu_ftp
    printer_model: str | None = None,  # noqa: ARG001 - accepted for call-site symmetry with bambu_ftp
    port: int | None = None,
) -> bool:
    """Upload a file to a Klipper printer's gcodes root via Moonraker.

    Signature mirrors bambu_ftp.upload_file_async — same first four
    positional args, same timeout-derives-from-file-size default, same
    UploadCancelled-on-deadline behavior — so it's a drop-in for
    with_ftp_retry(upload_file_async, ...) call sites in print_scheduler.py.
    """
    deadline = _upload_deadline(local_path) if timeout is None else timeout
    filename = remote_path.lstrip("/")
    url = f"{_base_url(ip_address, port)}/server/files/upload"

    try:
        total_size = local_path.stat().st_size
    except OSError:
        return False

    async def _do_upload() -> bool:
        async with _lock_for(ip_address):
            uploaded = 0

            async def _file_sender():
                nonlocal uploaded
                with open(local_path, "rb") as f:
                    while chunk := f.read(_UPLOAD_CHUNK_BYTES):
                        uploaded += len(chunk)
                        if progress_callback:
                            progress_callback(uploaded, total_size)
                        yield chunk

            with aiohttp.MultipartWriter("form-data") as mp:
                file_part = mp.append(_file_sender())
                file_part.set_content_disposition("form-data", name="file", filename=filename)
                root_part = mp.append("gcodes")
                root_part.set_content_disposition("form-data", name="root")

                async with (
                    aiohttp.ClientSession(headers=_headers(access_code)) as session,
                    session.post(url, data=mp) as resp,
                ):
                    if resp.status not in (200, 201):
                        logger.warning("Moonraker upload to %s failed: HTTP %s", ip_address, resp.status)
                        return False
                    return True

    try:
        return await asyncio.wait_for(_do_upload(), timeout=deadline)
    except TimeoutError as e:
        raise UploadCancelled(f"upload of {remote_path} exceeded its {deadline:.0f}s deadline") from e
    except aiohttp.ClientError as e:
        logger.warning("Moonraker upload to %s failed: %s", ip_address, e)
        return False


async def download_file_async(
    ip_address: str,
    access_code: str,
    remote_path: str,
    local_path: Path,
    timeout: float = 60.0,
    socket_timeout: float | None = None,  # noqa: ARG001 - call-site symmetry with bambu_ftp
    printer_model: str | None = None,  # noqa: ARG001 - call-site symmetry with bambu_ftp
    port: int | None = None,
) -> bool:
    """Download a file from a Klipper printer's gcodes root via Moonraker."""
    filename = remote_path.lstrip("/")
    url = f"{_base_url(ip_address, port)}/server/files/gcodes/{filename}"
    try:
        async with (
            aiohttp.ClientSession(
                headers=_headers(access_code), timeout=aiohttp.ClientTimeout(total=timeout)
            ) as session,
            session.get(url) as resp,
        ):
            if resp.status != 200:
                logger.warning("Moonraker download of %s from %s failed: HTTP %s", remote_path, ip_address, resp.status)
                return False
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(_UPLOAD_CHUNK_BYTES):
                    f.write(chunk)
            return True
    except (aiohttp.ClientError, TimeoutError) as e:
        logger.warning("Moonraker download of %s from %s failed: %s", remote_path, ip_address, e)
        return False


async def list_files_async(
    ip_address: str,
    access_code: str,
    path: str = "/",  # noqa: ARG001 - Moonraker's flat gcodes root has no subdirectory browsing here
    timeout: float = 30.0,
    socket_timeout: float | None = None,  # noqa: ARG001 - call-site symmetry with bambu_ftp
    printer_model: str | None = None,  # noqa: ARG001 - call-site symmetry with bambu_ftp
    port: int | None = None,
) -> list[dict]:
    """List files in a Klipper printer's gcodes root.

    Returns the same dict shape as BambuFTPClient.list_files:
    {"name", "is_directory", "size", "path", "mtime"?}.
    """
    url = f"{_base_url(ip_address, port)}/server/files/list"
    try:
        async with (
            aiohttp.ClientSession(
                headers=_headers(access_code), timeout=aiohttp.ClientTimeout(total=timeout)
            ) as session,
            session.get(url, params={"root": "gcodes"}) as resp,
        ):
            if resp.status != 200:
                return []
            data = await resp.json()
    except (aiohttp.ClientError, TimeoutError) as e:
        logger.info("Moonraker list_files failed for %s: %s", ip_address, e)
        return []

    files = []
    for entry in data.get("result", []):
        from datetime import datetime

        mtime = None
        if entry.get("modified") is not None:
            try:
                mtime = datetime.fromtimestamp(entry["modified"])
            except (OSError, OverflowError, ValueError):
                pass
        file_entry = {
            "name": entry.get("path", ""),
            "is_directory": False,  # Moonraker's flat /server/files/list only returns files, not dirs
            "size": entry.get("size", 0),
            "path": f"/{entry.get('path', '')}",
        }
        if mtime:
            file_entry["mtime"] = mtime
        files.append(file_entry)
    return files


async def delete_file_async(
    ip_address: str,
    access_code: str,
    remote_path: str,
    socket_timeout: float | None = None,  # noqa: ARG001 - call-site symmetry with bambu_ftp
    printer_model: str | None = None,  # noqa: ARG001 - call-site symmetry with bambu_ftp
    timeout: float = 60.0,
    port: int | None = None,
) -> DeleteResult:
    """Delete a file from a Klipper printer's gcodes root via Moonraker."""
    filename = remote_path.lstrip("/")
    url = f"{_base_url(ip_address, port)}/server/files/gcodes/{filename}"
    try:
        async with (
            aiohttp.ClientSession(
                headers=_headers(access_code), timeout=aiohttp.ClientTimeout(total=timeout)
            ) as session,
            session.delete(url) as resp,
        ):
            if resp.status == 200:
                return DeleteResult.DELETED
            if resp.status == 404:
                return DeleteResult.NOT_FOUND
            logger.warning("Moonraker delete of %s on %s failed: HTTP %s", remote_path, ip_address, resp.status)
            return DeleteResult.FAILED
    except (aiohttp.ClientError, TimeoutError) as e:
        logger.warning("Moonraker delete of %s on %s failed: %s", remote_path, ip_address, e)
        return DeleteResult.FAILED
