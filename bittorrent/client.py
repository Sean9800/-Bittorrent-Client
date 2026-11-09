"""Download orchestration: tracker loop + a pool of peer workers."""
from __future__ import annotations

import asyncio
import logging
import random
import sys
import time
from typing import Iterable, List, Optional, Set, Tuple

from . import tracker
from .peer import PeerConnection
from .pieces import PieceManager
from .torrent import Torrent

log = logging.getLogger(__name__)
Peer = Tuple[str, int]


class DownloadError(Exception):
    pass


def human_size(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


class TorrentClient:
    def __init__(self, torrent: Torrent, output_path: str, *, port: int = 6881,
                 max_peers: int = 30, extra_peers: Iterable[Peer] = (),
                 progress: bool = True):
        self.torrent = torrent
        self.port = port
        self.max_peers = max_peers
        self.extra_peers = list(extra_peers)
        self.show_progress = progress
        self.peer_id = b"-PY0001-" + bytes(random.choices(b"0123456789", k=12))
        self.manager = PieceManager(torrent, output_path)
        self.tiers: List[List[str]] = [list(t) for t in torrent.announce_list]
        self._queue: asyncio.Queue = asyncio.Queue()
        self._queued: Set[Peer] = set()
        self._connected: Set[Peer] = set()
        self._active = 0
        self._resumed_bytes = 0

    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        mgr = self.manager
        if mgr.check_existing():
            self._resumed_bytes = mgr.bytes_done
            log.info("resuming: %d/%d pieces already on disk",
                     mgr.pieces_done, self.torrent.num_pieces)
        if mgr.complete:
            mgr.close()
            return

        for p in self.extra_peers:
            self._enqueue(p)
        tasks = [asyncio.create_task(self._worker()) for _ in range(self.max_peers)]
        if self.tiers:
            tasks.append(asyncio.create_task(self._tracker_loop()))
        try:
            await self._wait_until_done()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.tiers:
                await self._final_announce()
            mgr.close()

    # -- peers -------------------------------------------------------------- #
    def _enqueue(self, peer: Peer) -> None:
        if peer not in self._queued and peer not in self._connected:
            self._queued.add(peer)
            self._queue.put_nowait(peer)

    async def _worker(self) -> None:
        while True:
            peer = await self._queue.get()
            self._queued.discard(peer)
            if peer in self._connected:
                continue
            self._connected.add(peer)
            self._active += 1
            try:
                await PeerConnection(*peer, self.torrent, self.peer_id, self.manager).run()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("peer %s:%d dropped: %s: %s", *peer, type(e).__name__, e)
            finally:
                self._connected.discard(peer)
                self._active -= 1

    # -- tracker ------------------------------------------------------------- #
    async def _announce(self, event: Optional[str]) -> Optional[tracker.TrackerResponse]:
        mgr = self.manager
        for tier in self.tiers:
            urls = tier[:]
            random.shuffle(urls)
            for url in urls:
                try:
                    return await tracker.announce(
                        url, self.torrent, self.peer_id, self.port,
                        uploaded=0,
                        downloaded=mgr.bytes_done - self._resumed_bytes,
                        left=self.torrent.length - mgr.bytes_done,
                        event=event)
                except Exception as e:
                    log.warning("tracker %s failed: %s", url, e)
        return None

    async def _tracker_loop(self) -> None:
        event: Optional[str] = "started"
        while True:
            resp = await self._announce(event)
            wait = 30
            if resp:
                event = None
                log.info("tracker returned %d peers", len(resp.peers))
                for p in resp.peers:
                    self._enqueue(p)
                wait = max(30, min(resp.interval, 1800)) if resp.peers else 30
            await asyncio.sleep(wait)

    async def _final_announce(self) -> None:
        events = (["completed"] if self.manager.complete and
                  self.manager.bytes_done > self._resumed_bytes else []) + ["stopped"]
        for ev in events:
            try:
                await asyncio.wait_for(self._announce(ev), 5)
            except Exception:
                pass

    # -- progress -------------------------------------------------------------- #
    async def _wait_until_done(self) -> None:
        mgr, total = self.manager, self.torrent.length
        last_bytes, last_t, speed = mgr.bytes_done, time.monotonic(), 0.0
        last_print = 0.0
        tty = sys.stderr.isatty()
        while not mgr.complete:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            if now - last_t >= 1:
                inst = (mgr.bytes_done - last_bytes) / (now - last_t)
                speed = inst if speed == 0 else 0.7 * speed + 0.3 * inst
                last_bytes, last_t = mgr.bytes_done, now
            if self.show_progress and now - last_print >= (0.5 if tty else 5):
                last_print = now
                pct = 100 * mgr.bytes_done / total if total else 100
                eta = (total - mgr.bytes_done) / speed if speed > 1 else None
                eta_s = f"{int(eta // 60)}m{int(eta % 60):02d}s" if eta is not None else "--"
                line = (f"{pct:5.1f}%  {human_size(mgr.bytes_done)}/{human_size(total)}  "
                        f"{human_size(speed)}/s  peers: {self._active}  ETA {eta_s}")
                print("\r" + line + "   " if tty else line, end="" if tty else "\n",
                      file=sys.stderr, flush=True)
            if (not mgr.complete and not self.tiers
                    and self._queue.empty() and self._active == 0):
                raise DownloadError("no peers left to try; download incomplete")
        if self.show_progress and tty:
            print(file=sys.stderr)