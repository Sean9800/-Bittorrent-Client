"""One TCP connection to one peer (BEP 3 peer wire protocol).

Handshake:   <pstrlen=19><"BitTorrent protocol"><8 reserved><info_hash><peer_id>
Messages:    <4-byte length><1-byte id><payload>   (length 0 = keep-alive)

This client only downloads: it never sends HAVE/BITFIELD and ignores REQUESTs.
"""
from __future__ import annotations

import asyncio
import logging
import struct
import time
from collections import deque
from typing import Optional, Set

from .pieces import PieceManager
from .torrent import Torrent

log = logging.getLogger(__name__)

PSTR = b"BitTorrent protocol"
HANDSHAKE_LEN = 49 + len(PSTR)  # 68

CHOKE, UNCHOKE, INTERESTED, NOT_INTERESTED, HAVE, BITFIELD, REQUEST, PIECE, CANCEL = range(9)

BLOCK_SIZE = 16 * 1024      # de-facto maximum request size
MAX_PIPELINE = 8            # outstanding requests per peer
CONNECT_TIMEOUT = 10
POLL_INTERVAL = 5           # how often an idle connection re-checks for work
IDLE_TIMEOUT = 120          # drop peers that send nothing (not even keep-alives)
MAX_MESSAGE = 4 * 1024 * 1024
MAX_BAD_PIECES = 3


class PeerError(Exception):
    pass


class _PieceDownload:
    """In-progress download of a single piece, block by block."""

    def __init__(self, index: int, length: int):
        self.index = index
        self.length = length
        self.buf = bytearray(length)
        self.todo = deque(range(0, length, BLOCK_SIZE))   # offsets not yet requested
        self.inflight: Set[int] = set()                    # requested, not yet received
        self.remaining = len(self.todo)

    def block_length(self, offset: int) -> int:
        return min(BLOCK_SIZE, self.length - offset)

    def requeue_inflight(self) -> None:
        self.todo = deque(sorted(set(self.todo) | self.inflight))
        self.inflight.clear()


class PeerConnection:
    def __init__(self, host: str, port: int, torrent: Torrent, peer_id: bytes,
                 manager: PieceManager):
        self.host, self.port = host, port
        self.torrent, self.peer_id, self.manager = torrent, peer_id, manager
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.choked = True
        self.have: Set[int] = set()
        self.current: Optional[_PieceDownload] = None
        self.bad_pieces = 0

    def __repr__(self) -> str:
        return f"<Peer {self.host}:{self.port}>"

    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), CONNECT_TIMEOUT)
            await self._handshake()
            await self._send(INTERESTED)
            await self._loop()
        finally:
            await self._cleanup()

    async def _cleanup(self) -> None:
        self.manager.peer_lost(self.have)
        self.have = set()
        if self.current is not None:
            self.manager.release(self.current.index)
            self.current = None
        if self.writer is not None:
            self.writer.close()
            try:
                await asyncio.wait_for(self.writer.wait_closed(), 1)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    async def _handshake(self) -> None:
        self.writer.write(bytes([len(PSTR)]) + PSTR + bytes(8)
                          + self.torrent.info_hash + self.peer_id)
        await self.writer.drain()
        resp = await asyncio.wait_for(self.reader.readexactly(HANDSHAKE_LEN), CONNECT_TIMEOUT)
        if resp[0] != len(PSTR) or resp[1:20] != PSTR:
            raise PeerError("not a BitTorrent peer")
        if resp[28:48] != self.torrent.info_hash:
            raise PeerError("peer is serving a different torrent")
        log.debug("%r handshake ok (peer id %r)", self, resp[48:56])

    async def _send(self, msg_id: int, payload: bytes = b"") -> None:
        self.writer.write(struct.pack(">IB", 1 + len(payload), msg_id) + payload)
        await self.writer.drain()

    # ------------------------------------------------------------------ #
    async def _loop(self) -> None:
        last_seen = time.monotonic()
        while not self.manager.complete:
            try:
                # Cancelling readexactly() on timeout is safe: unread bytes stay
                # buffered in the StreamReader, so the stream never desyncs.
                header = await asyncio.wait_for(self.reader.readexactly(4), POLL_INTERVAL)
            except asyncio.TimeoutError:
                if time.monotonic() - last_seen > IDLE_TIMEOUT:
                    raise PeerError("idle timeout")
                await self._pump()  # maybe work freed up while we were idle
                continue
            last_seen = time.monotonic()
            (length,) = struct.unpack(">I", header)
            if length == 0:
                continue  # keep-alive
            if length > MAX_MESSAGE:
                raise PeerError(f"oversized message ({length} bytes)")
            payload = await asyncio.wait_for(self.reader.readexactly(length), 30)
            await self._handle(payload[0], payload[1:])
            await self._pump()

    async def _handle(self, msg_id: int, payload: bytes) -> None:
        n = self.torrent.num_pieces
        if msg_id == CHOKE:
            self.choked = True
            if self.current:  # peer discards our pending requests
                self.current.requeue_inflight()
        elif msg_id == UNCHOKE:
            self.choked = False
        elif msg_id == HAVE:
            if len(payload) != 4:
                raise PeerError("bad HAVE")
            (idx,) = struct.unpack(">I", payload)
            if idx >= n:
                raise PeerError("HAVE index out of range")
            if idx not in self.have:
                self.have.add(idx)
                self.manager.peer_gained(idx)
        elif msg_id == BITFIELD:
            if len(payload) < (n + 7) // 8:
                raise PeerError("bitfield too short")
            new = {i for i in range(n) if payload[i >> 3] & (0x80 >> (i & 7))}
            self.manager.peer_lost(self.have)
            for i in new:
                self.manager.peer_gained(i)
            self.have = new
        elif msg_id == PIECE:
            self._on_piece(payload)
        # INTERESTED / NOT_INTERESTED / REQUEST / CANCEL: we don't upload.

    # ------------------------------------------------------------------ #
    def _on_piece(self, payload: bytes) -> None:
        if len(payload) < 8:
            raise PeerError("bad PIECE")
        index, begin = struct.unpack(">II", payload[:8])
        block = payload[8:]
        cur = self.current
        if cur is None or index != cur.index or begin not in cur.inflight:
            return  # stale block (e.g. after a choke or endgame cancel)
        if len(block) != cur.block_length(begin):
            raise PeerError("block has wrong length")
        cur.buf[begin:begin + len(block)] = block
        cur.inflight.discard(begin)
        cur.remaining -= 1
        if cur.remaining == 0:
            self.current = None
            if self.manager.submit(cur.index, bytes(cur.buf)):
                log.debug("%r piece %d verified", self, cur.index)
            else:
                self.bad_pieces += 1
                log.warning("%r sent corrupt piece %d", self, cur.index)
                if self.bad_pieces >= MAX_BAD_PIECES:
                    raise PeerError("too many corrupt pieces")

    async def _pump(self) -> None:
        """Keep the request pipeline full."""
        if self.choked:
            return
        if self.current and self.manager.have[self.current.index]:
            self.manager.release(self.current.index)   # finished elsewhere (endgame)
            self.current = None
        if self.current is None:
            idx = self.manager.claim(self.have)
            if idx is None:
                return
            self.current = _PieceDownload(idx, self.torrent.piece_size(idx))
        cur = self.current
        while cur.todo and len(cur.inflight) < MAX_PIPELINE:
            offset = cur.todo.popleft()
            cur.inflight.add(offset)
            await self._send(REQUEST, struct.pack(">III", cur.index, offset,
                                                  cur.block_length(offset)))