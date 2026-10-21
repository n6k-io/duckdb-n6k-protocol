"""Mirrors the kernel relay that launches it: a probe connection closed before HELLO, then
several clients at once, each speaking raw n6k stream framing."""

from __future__ import annotations

import io
import os
import socket
import stat
import struct
import subprocess
import tempfile
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pyarrow.ipc as ipc
import pytest

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_PING,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    N6K_PROTOCOL_VERSION,
    OP_QUERY,
    STREAM_FRAME_N6K,
)

from _paths import REPO_ROOT

DUCKDB_BIN = os.path.join(REPO_ROOT, "build", "release", "duckdb")

SEED = (
    "CREATE TABLE users (id INTEGER, name VARCHAR);"
    "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');"
)


def _short_socket_path() -> str:
    # AF_UNIX paths are capped near 104 bytes; pytest's tmp_path is often longer on macOS.
    return os.path.join(tempfile.mkdtemp(dir="/tmp", prefix="n6k-"), "s.sock")


def _clean_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "N6K_LISTEN"}
    env.update(extra)
    return env


def _spawn(path: str, call: str, env: dict[str, str]) -> subprocess.Popen[bytes]:
    if not os.path.exists(DUCKDB_BIN):
        pytest.skip(f"not built: {DUCKDB_BIN} (run `make release`)")
    proc = subprocess.Popen(
        [DUCKDB_BIN, "-unsigned", "-c", f"{SEED} {call}"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 30
    while True:
        try:
            with socket.socket(socket.AF_UNIX) as probe:
                probe.connect(path)
            return proc
        except OSError:
            pass
        if proc.poll() is not None:
            out = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
            raise RuntimeError(f"duckdb exited before listening:\n{out[:2000]}")
        if time.time() > deadline:
            proc.kill()
            raise RuntimeError("n6k_serve_socket never started listening")
        time.sleep(0.05)


def _stop(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="module")
def listening() -> Iterator[str]:
    path = _short_socket_path()
    proc = _spawn(path, "CALL n6k_serve_socket('memory');", _clean_env(N6K_LISTEN=path))
    try:
        yield path
    finally:
        _stop(proc)


class _Client:
    def __init__(self, path: str) -> None:
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(10)
        self.sock.connect(path)

    def close(self) -> None:
        self.sock.close()

    def send(self, header: dict) -> None:
        payload = pack_frame(header)
        self.sock.sendall(struct.pack(">IB", len(payload), STREAM_FRAME_N6K) + payload)

    def recv(self) -> tuple[dict, bytes]:
        while True:
            n, frame_type = struct.unpack(">IB", self._exactly(5))
            payload = self._exactly(n)
            assert frame_type == STREAM_FRAME_N6K, frame_type
            header, body = unpack_frame(payload)
            if header["t"] != FT_PING:
                return dict(header), bytes(body)

    def open(self) -> "_Client":
        self.send({"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION})
        assert self.recv()[0]["t"] == FT_HELLO_ACK
        self.send({"t": FT_OPEN, "ns": 1, "catalog": "memory"})
        opened = self.recv()[0]
        assert opened["t"] == FT_OPEN_ACK, opened
        return self

    def query(self, sql: str, req_id: int = 1) -> dict:
        self.send({"t": FT_REQ, "id": req_id, "op": OP_QUERY, "sql": sql, "ns": 1})
        parts = []
        while True:
            header, body = self.recv()
            assert header.get("id") == req_id, header
            if header["t"] in (FT_RESP_SCHEMA, FT_RESP_CHUNK):
                parts.append(body)
            elif header["t"] == FT_RESP_END:
                return ipc.open_stream(io.BytesIO(b"".join(parts))).read_all().to_pydict()
            elif header["t"] == FT_ERR:
                raise AssertionError(f"server error: {header.get('exception_message')!r}")
            else:
                raise AssertionError(f"unexpected frame type {header['t']}")

    def _exactly(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            assert chunk, "server closed the socket"
            buf += chunk
        return buf


def _count_users(path: str) -> dict:
    client = _Client(path).open()
    try:
        return client.query("SELECT count(*) AS c FROM users")
    finally:
        client.close()


def test_socket_file_is_owner_only(listening: str) -> None:
    assert stat.S_IMODE(os.stat(listening).st_mode) == 0o600


def test_probe_then_query(listening: str) -> None:
    with socket.socket(socket.AF_UNIX) as probe:
        probe.connect(listening)
    assert _count_users(listening) == {"c": [3]}


def test_serves_many_clients_concurrently(listening: str) -> None:
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(lambda _: _count_users(listening), range(4)))
    assert results == [{"c": [3]}] * 4


def test_server_outlives_a_disconnect(listening: str) -> None:
    _count_users(listening)
    assert _count_users(listening) == {"c": [3]}


def test_listen_parameter_reclaims_a_stale_file_and_unlinks_on_exit() -> None:
    path = _short_socket_path()
    with socket.socket(socket.AF_UNIX) as stale:
        stale.bind(path)
    proc = _spawn(path, f"CALL n6k_serve_socket('memory', listen => '{path}');", _clean_env())
    try:
        assert _count_users(path) == {"c": [3]}
        proc.send_signal(2)
        proc.wait(timeout=10)
    finally:
        _stop(proc)
    assert not os.path.exists(path)


def test_refuses_a_path_a_live_server_holds(listening: str) -> None:
    proc = subprocess.run(
        [DUCKDB_BIN, "-unsigned", "-c", "CALL n6k_serve_socket('memory');"],
        env=_clean_env(N6K_LISTEN=listening),
        capture_output=True,
        timeout=30,
    )
    assert b"in use by another server" in proc.stdout + proc.stderr


def test_refuses_without_a_listen_path() -> None:
    if not os.path.exists(DUCKDB_BIN):
        pytest.skip(f"not built: {DUCKDB_BIN} (run `make release`)")
    proc = subprocess.run(
        [DUCKDB_BIN, "-unsigned", "-c", "CALL n6k_serve_socket('memory');"],
        env=_clean_env(),
        capture_output=True,
        timeout=30,
    )
    assert b"neither listen => nor N6K_LISTEN is set" in proc.stdout + proc.stderr
