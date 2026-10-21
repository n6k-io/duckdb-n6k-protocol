"""The frame codec: pack_frame / unpack_frame / validate_header."""

import pytest

from n6k_protocol.engine import pack_frame, unpack_frame, validate_header
from n6k_protocol.protocol import (
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_PENDING,
    FT_REQ,
    OP_INSERT,
    OP_QUERY,
    OPEN_PENDING_INTERVAL_MS,
    N6K_PROTOCOL_VERSION,
    OPEN_PENDING_STALL_MS,
    frame_type_name,
)


def test_round_trip_without_body():
    header = {"t": FT_REQ, "id": 1, "op": OP_QUERY, "sql": "select 1"}
    decoded, body = unpack_frame(pack_frame(header))
    assert decoded == header
    assert len(body) == 0


def test_round_trip_with_arrow_body():
    arrow = b"ARROW\x00BYTES"
    header = {"t": FT_REQ, "id": 2, "op": OP_INSERT, "schema": "s", "table": "t"}
    decoded, body = unpack_frame(pack_frame(header, arrow))
    assert decoded == header
    assert bytes(body) == arrow


def test_unknown_frame_type_is_rejected():
    with pytest.raises(ValueError, match="unknown frame type"):
        validate_header({"t": 250})


def test_missing_required_field_is_rejected():
    with pytest.raises(ValueError):
        pack_frame({"t": FT_REQ, "op": OP_QUERY, "sql": "select 1"})


def test_hello_without_token_is_rejected():
    with pytest.raises(ValueError):
        pack_frame({"t": FT_HELLO, "protocol_version": N6K_PROTOCOL_VERSION})


def test_hello_without_protocol_version_is_rejected():
    with pytest.raises(ValueError):
        pack_frame({"t": FT_HELLO, "token": "x"})


@pytest.mark.parametrize(
    "header",
    [
        {"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION},
        {"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["foo"]},
        {"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": []},
        {"t": FT_OPEN, "ns": 1, "catalog": "db", "capabilities": ["foo"]},
        {"t": FT_OPEN_ACK, "ns": 1, "max_concurrent_reqs": 1, "default_batch_credits": 1, "capabilities": []},
    ],
)
def test_capabilities_headers_are_accepted(header):
    validate_header(header)


@pytest.mark.parametrize(
    "header",
    [
        {"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION},
        {"t": FT_OPEN_ACK, "ns": 1, "max_concurrent_reqs": 1, "default_batch_credits": 1},
    ],
)
def test_acks_without_capabilities_are_rejected(header):
    with pytest.raises(ValueError):
        validate_header(header)


def test_short_frame_is_rejected():
    with pytest.raises(ValueError, match="short frame"):
        unpack_frame(b"")


def test_open_pending_carries_ns_and_an_optional_message():
    validate_header({"t": FT_OPEN_PENDING, "ns": 1})
    validate_header({"t": FT_OPEN_PENDING, "ns": 1, "message": "starting"})
    assert frame_type_name(FT_OPEN_PENDING) == "OPEN_PENDING"


@pytest.mark.parametrize("header", [{"t": FT_OPEN_PENDING}, {"t": FT_OPEN_PENDING, "ns": 1, "id": 3}])
def test_open_pending_without_ns_or_with_extra_keys_is_rejected(header):
    with pytest.raises(ValueError):
        validate_header(header)


def test_a_client_tolerates_at_least_one_missed_open_pending():
    assert OPEN_PENDING_STALL_MS >= 2 * OPEN_PENDING_INTERVAL_MS
