"""WebSocket ⇄ Unix-socket relay: lets a `duckdb` client ATTACH over a WebSocket to a worker that
serves n6k stream framing on a Unix socket (`n6k_server.stream.serve_unix`). Each WebSocket gets
its own connection to the worker; binary messages are `STREAM_FRAME_N6K`, text `STREAM_FRAME_JSONRPC`.
"""

from __future__ import annotations

import asyncio
import logging

from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from n6k_protocol.protocol import STREAM_FRAME_JSONRPC, STREAM_FRAME_N6K
from n6k_server.stream import read_frame, write_frame

logger = logging.getLogger("n6k_server.ws_relay")


async def _ws_to_unix(ws: WebSocket, writer: asyncio.StreamWriter) -> None:
    while True:
        try:
            message = await ws.receive()
        except (WebSocketDisconnect, RuntimeError):
            return
        if message["type"] == "websocket.disconnect":
            return
        if message.get("bytes") is not None:
            write_frame(writer, STREAM_FRAME_N6K, message["bytes"])
        else:
            write_frame(writer, STREAM_FRAME_JSONRPC, message.get("text", "").encode())
        await writer.drain()


async def _unix_to_ws(reader: asyncio.StreamReader, ws: WebSocket) -> None:
    while True:
        frame = await read_frame(reader)
        if frame is None:
            return
        frame_type, payload = frame
        if frame_type not in (STREAM_FRAME_N6K, STREAM_FRAME_JSONRPC):
            logger.error("closing: unknown stream frame type 0x%02x from the worker", frame_type)
            return
        try:
            if frame_type == STREAM_FRAME_JSONRPC:
                await ws.send_text(payload.decode())
            else:
                await ws.send_bytes(payload)
        except (WebSocketDisconnect, RuntimeError):
            return


def build_relay_app(socket_path: str) -> Starlette:
    async def relay(ws: WebSocket) -> None:
        await ws.accept()
        try:
            reader, writer = await asyncio.open_unix_connection(socket_path)
        except OSError as exc:
            logger.error("could not dial unix:%s: %s", socket_path, exc)
            await ws.close(code=1011, reason="worker unreachable")
            return
        logger.info("duckdb attached at %s; dialed unix:%s", ws.url.path, socket_path)
        t_in = asyncio.create_task(_ws_to_unix(ws, writer))
        t_out = asyncio.create_task(_unix_to_ws(reader, ws))
        try:
            await asyncio.wait({t_in, t_out}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (t_in, t_out):
                t.cancel()
            writer.close()
            try:
                await ws.close()
            except Exception:
                pass
            logger.info("relay session ended")

    return Starlette(
        routes=[
            WebSocketRoute("/ws", relay),
            WebSocketRoute("/{prefix:path}/ws", relay),
        ],
    )


def run_relay(socket_path: str, host: str, port: int, log_level: str = "info") -> None:
    import uvicorn

    app = build_relay_app(socket_path)
    uvicorn.run(app, host=host, port=port, log_level=log_level)
