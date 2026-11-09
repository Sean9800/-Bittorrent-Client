"""Tracker communication: HTTP(S) (BEP 3 / BEP 23 compact) and UDP (BEP 15)."""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import random
import socket
import struct
import urllib.request
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from urllib.parse import quote_from_bytes, urlparse

from . import bencode
from .torrent import Torrent

log = logging.getLogger(__name__)

Peer = Tuple[str, int]
_UDP_MAGIC = 0x41727101980
_EVENT_CODES = {None: 0, "completed": 1, "started": 2, "stopped": 3}


class TrackerError(Exception):
    pass


@dataclass
class TrackerResponse:
    interval: int = 1800
    peers: List[Peer] = field(default_factory=list)


def parse_compact_peers(blob: bytes, v6: bool = False) -> List[Peer]:
    size = 18 if v6 else 6
    peers = []
    for i in range(0, len(blob) - len(blob) % size, size):
        if v6:
            ip = str(ipaddress.IPv6Address(blob[i:i + 16]))
        else:
            ip = socket.inet_ntoa(blob[i:i + 4])
        (port,) = struct.unpack(">H", blob[i + size - 2:i + size])
        if port:
            peers.append((ip, port))
    return peers


async def announce(url: str, torrent: Torrent, peer_id: bytes, port: int,
                   uploaded: int, downloaded: int, left: int,
                   event: Optional[str] = None, timeout: float = 15) -> TrackerResponse:
    scheme = urlparse(url).scheme
    args = (url, torrent, peer_id, port, uploaded, downloaded, left, event, timeout)
    if scheme in ("http", "https"):
        return await _http_announce(*args)
    if scheme == "udp":
        return await _udp_announce(*args)
    raise TrackerError(f"unsupported tracker scheme {scheme!r}")


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
async def _http_announce(url, torrent, peer_id, port, uploaded, downloaded,
                         left, event, timeout) -> TrackerResponse:
    query = (
        f"info_hash={quote_from_bytes(torrent.info_hash, safe='')}"
        f"&peer_id={quote_from_bytes(peer_id, safe='')}"
        f"&port={port}&uploaded={uploaded}&downloaded={downloaded}"
        f"&left={left}&compact=1&numwant=50"
    )
    if event:
        query += f"&event={event}"
    full = url + ("&" if "?" in url else "?") + query

    def fetch() -> bytes:
        req = urllib.request.Request(full, headers={"User-Agent": "pytorrent/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()

    loop = asyncio.get_running_loop()
    try:
        body = await loop.run_in_executor(None, fetch)
    except OSError as e:  # URLError / HTTPError / socket errors
        raise TrackerError(f"HTTP request failed: {e}") from e
    try:
        data = bencode.decode(body)
    except bencode.BencodeError as e:
        raise TrackerError(f"bad tracker response: {e}") from e
    if not isinstance(data, dict):
        raise TrackerError("tracker response is not a dictionary")
    if b"failure reason" in data:
        raise TrackerError(data[b"failure reason"].decode("utf-8", "replace"))

    peers_field = data.get(b"peers", b"")
    peers: List[Peer] = []
    if isinstance(peers_field, bytes):
        peers = parse_compact_peers(peers_field)
    else:  # non-compact: list of {ip, port}
        for p in peers_field:
            try:
                peers.append((p[b"ip"].decode(), int(p[b"port"])))
            except (KeyError, ValueError, AttributeError):
                continue
    if isinstance(data.get(b"peers6"), bytes):
        peers += parse_compact_peers(data[b"peers6"], v6=True)
    return TrackerResponse(interval=int(data.get(b"interval", 1800)), peers=peers)


# --------------------------------------------------------------------------- #
# UDP
# --------------------------------------------------------------------------- #
class _UdpProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()

    def datagram_received(self, data, addr):
        self.queue.put_nowait(data)

    def error_received(self, exc):
        self.queue.put_nowait(exc)


async def _udp_request(transport, proto, packet: bytes, tx_id: int,
                       expect_action: int, min_len: int, timeout: float) -> bytes:
    for attempt in range(2):
        transport.sendto(packet)
        try:
            deadline = asyncio.get_running_loop().time() + timeout * (attempt + 1)
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                resp = await asyncio.wait_for(proto.queue.get(), max(remaining, 0.01))
                if isinstance(resp, Exception):
                    raise TrackerError(f"UDP error: {resp}")
                if len(resp) < 8:
                    continue
                action, rtx = struct.unpack(">II", resp[:8])
                if rtx != tx_id:
                    continue
                if action == 3:
                    raise TrackerError(resp[8:].decode("utf-8", "replace"))
                if action == expect_action and len(resp) >= min_len:
                    return resp
        except asyncio.TimeoutError:
            continue
    raise TrackerError("UDP tracker timed out")


async def _udp_announce(url, torrent, peer_id, port, uploaded, downloaded,
                        left, event, timeout) -> TrackerResponse:
    parsed = urlparse(url)
    if not parsed.hostname or not parsed.port:
        raise TrackerError("UDP tracker URL needs host and port")
    loop = asyncio.get_running_loop()
    try:
        transport, proto = await loop.create_datagram_endpoint(
            _UdpProtocol, remote_addr=(parsed.hostname, parsed.port))
    except OSError as e:
        raise TrackerError(f"cannot reach UDP tracker: {e}") from e
    per_try = max(timeout / 3, 1)
    try:
        # 1) connect handshake -> connection_id
        tx = random.getrandbits(32)
        resp = await _udp_request(
            transport, proto, struct.pack(">QII", _UDP_MAGIC, 0, tx), tx, 0, 16, per_try)
        (conn_id,) = struct.unpack(">Q", resp[8:16])

        # 2) announce
        tx = random.getrandbits(32)
        packet = struct.pack(
            ">QII20s20sQQQIIIiH",
            conn_id, 1, tx, torrent.info_hash, peer_id,
            downloaded, left, uploaded,
            _EVENT_CODES[event], 0, random.getrandbits(32), -1, port)
        resp = await _udp_request(transport, proto, packet, tx, 1, 20, per_try)
        interval = struct.unpack(">I", resp[8:12])[0]
        return TrackerResponse(interval=interval, peers=parse_compact_peers(resp[20:]))
    finally:
        transport.close()