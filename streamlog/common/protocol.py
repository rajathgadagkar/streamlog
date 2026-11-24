"""
streamlog.common.protocol
=========================
JSON-over-length-prefix framing for all node-to-node and client-to-node
communication over TCP.

Wire format (per message):
  [ 4-byte unsigned int, big-endian — payload length ]
  [ payload bytes — UTF-8 encoded JSON ]
"""

from __future__ import annotations

import json
import socket
import struct

_HEADER_FMT = ">I"  # big-endian unsigned 32-bit integer
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)


def send_msg(sock: socket.socket, data: dict) -> None:
    """Serialize *data* to JSON and send it over *sock* with a 4-byte length prefix."""
    payload = json.dumps(data).encode("utf-8")
    header = struct.pack(_HEADER_FMT, len(payload))
    sock.sendall(header + payload)


def recv_msg(sock: socket.socket) -> dict:
    """
    Receive one framed message from *sock*.

    Returns the decoded dict, or raises:
      - ``ConnectionError`` if the peer closed the connection.
      - ``json.JSONDecodeError`` if the payload is malformed.
    """
    header = _recv_exact(sock, _HEADER_SIZE)
    if not header:
        raise ConnectionError("Connection closed by remote peer (header)")

    (length,) = struct.unpack(_HEADER_FMT, header)
    raw = _recv_exact(sock, length)
    if not raw:
        raise ConnectionError("Connection closed by remote peer (payload)")

    return json.loads(raw.decode("utf-8"))


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly *n* bytes from *sock*; return b'' on clean close."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""
        buf.extend(chunk)
    return bytes(buf)
