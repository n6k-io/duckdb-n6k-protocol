"""n6k stream framing (`STREAM_HEADER`) over an asyncio byte stream, and a Unix-socket server that
hands each client to the pump as a `WebSocketLike`."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Tuple

from n6k_protocol.protocol import STREAM_FRAME_JSONRPC, STREAM_FRAME_N6K, STREAM_HEADER

logger = logging.getLogger("n6k_server.stream")


class FrameTooLarge(ValueError):
    pass


async def read_frame(reader: asyncio.StreamReader, max_bytes: Optional[int] = None) -> Optional[Tuple[int, bytes]]:
    """The next `(frame_type, payload)`, or None if the stream ends, including mid-frame. Raises
    `FrameTooLarge` if the declared length exceeds `max_bytes`."""
    try:
        header = await reader.readexactly(STREAM_HEADER.size)
        length, frame_type = STREAM_HEADER.unpack(header)
        if max_bytes is not None and length > max_bytes:
            raise FrameTooLarge(f"frame of {length} bytes exceeds {max_bytes}")
        payload = await reader.readexactly(length) if length else b""
    except asyncio.IncompleteReadError:
        return None
    return frame_type, payload


def write_frame(writer: asyncio.StreamWriter, frame_type: int, payload: bytes) -> None:
    writer.write(STREAM_HEADER.pack(len(payload), frame_type) + payload)


class StreamTransport:
    """A `WebSocketLike` over a framed stream: `STREAM_FRAME_N6K` is binary, `STREAM_FRAME_JSONRPC`
    is text, end of stream is a disconnect."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._closed = False

    async def receive(self) -> dict[str, Any]:
        frame = await read_frame(self._reader)
        if frame is None:
            return {"type": "websocket.disconnect", "code": 1000}
        frame_type, payload = frame
        if frame_type == STREAM_FRAME_JSONRPC:
            return {"type": "websocket.receive", "text": payload.decode()}
        if frame_type == STREAM_FRAME_N6K:
            return {"type": "websocket.receive", "bytes": payload}
        raise ValueError(f"unknown stream frame type 0x{frame_type:02x}")

    async def send_bytes(self, data: bytes) -> None:
        write_frame(self._writer, STREAM_FRAME_N6K, data)
        await self._writer.drain()

    async def send_text(self, data: str) -> None:
        write_frame(self._writer, STREAM_FRAME_JSONRPC, data.encode())
        await self._writer.drain()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except Exception:
            logger.debug("socket close on already-closed transport", exc_info=True)


def _claim_socket_path(path: str) -> None:
    """Remove a stale socket file at `path`; raise if a server is live on it."""
    if not os.path.exists(path):
        return
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(path)
    except (ConnectionRefusedError, FileNotFoundError):
        os.unlink(path)
        return
    finally:
        probe.close()
    raise ValueError(f"{path} is in use by another server")


async def serve_unix(path: str, on_conn: Callable[[StreamTransport], Awaitable[None]]) -> None:
    """Listen on `path` and run `on_conn` per client until cancelled. The socket is chmod 0600 only
    after it is bound, so it briefly has the umask's permissions."""
    _claim_socket_path(path)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        transport = StreamTransport(reader, writer)
        try:
            await on_conn(transport)
        except Exception:
            logger.exception("socket client failed")
        finally:
            await transport.close()

    server = await asyncio.start_unix_server(handle, path)
    os.chmod(path, 0o600)
    logger.info("listening on %s", path)
    try:
        await server.serve_forever()
    finally:
        server.close()
        Path(path).unlink(missing_ok=True)
