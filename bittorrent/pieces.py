"""Piece bookkeeping, hash verification and disk I/O."""
from __future__ import annotations

import hashlib
import logging
import os
import random
from typing import Dict, Iterable, Optional, Set

from .torrent import Torrent

log = logging.getLogger(__name__)


class PieceManager:
    def __init__(self, torrent: Torrent, path: str):
        self.torrent = torrent
        self.path = path
        n = torrent.num_pieces
        self.have = [False] * n
        self.availability = [0] * n          # how many connected peers have each piece
        self.in_progress: Dict[int, int] = {}  # piece -> number of peers fetching it
        self.pieces_done = 0
        self.bytes_done = 0
        self._file = self._open_file()

    # -- file handling --------------------------------------------------- #
    def _open_file(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        mode = "r+b" if os.path.exists(self.path) else "w+b"
        f = open(self.path, mode)
        f.truncate(self.torrent.length)
        return f

    def close(self) -> None:
        if not self._file.closed:
            self._file.flush()
            self._file.close()

    @property
    def complete(self) -> bool:
        return self.pieces_done == self.torrent.num_pieces

    def check_existing(self) -> int:
        """Hash whatever is already on disk so an interrupted download resumes."""
        t = self.torrent
        for i in range(t.num_pieces):
            size = t.piece_size(i)
            self._file.seek(i * t.piece_length)
            data = self._file.read(size)
            if len(data) == size and hashlib.sha1(data).digest() == t.piece_hashes[i]:
                self._mark_done(i)
        return self.pieces_done

    def _mark_done(self, index: int) -> None:
        self.have[index] = True
        self.pieces_done += 1
        self.bytes_done += self.torrent.piece_size(index)

    # -- peer availability (for rarest-first) ---------------------------- #
    def peer_gained(self, index: int) -> None:
        self.availability[index] += 1

    def peer_lost(self, pieces: Iterable[int]) -> None:
        for i in pieces:
            self.availability[i] -= 1

    # -- work assignment -------------------------------------------------- #
    def claim(self, peer_has: Set[int]) -> Optional[int]:
        """Pick a piece for a peer to download, rarest first.

        Normally we only hand out pieces nobody is working on. In "endgame"
        (every missing piece is already being fetched) we let idle peers
        duplicate in-flight pieces, so one slow peer can't stall the finish.
        """
        candidates = [i for i in peer_has
                      if not self.have[i] and i not in self.in_progress]
        if candidates:
            rarest = min(self.availability[i] for i in candidates)
            pick = random.choice([i for i in candidates
                                  if self.availability[i] == rarest])
        else:
            missing = [i for i, h in enumerate(self.have) if not h]
            if not missing or any(i not in self.in_progress for i in missing):
                return None
            dupes = [i for i in peer_has if not self.have[i]]
            if not dupes:
                return None
            pick = min(dupes, key=lambda i: self.in_progress[i])
        self.in_progress[pick] = self.in_progress.get(pick, 0) + 1
        return pick

    def release(self, index: int) -> None:
        left = self.in_progress.get(index, 0) - 1
        if left > 0:
            self.in_progress[index] = left
        else:
            self.in_progress.pop(index, None)

    # -- completion -------------------------------------------------------- #
    def submit(self, index: int, data: bytes) -> bool:
        """Verify + store a finished piece. Returns False on hash mismatch."""
        self.release(index)
        if self.have[index]:
            return True  # someone else got there first (endgame)
        t = self.torrent
        if len(data) != t.piece_size(index) or \
                hashlib.sha1(data).digest() != t.piece_hashes[index]:
            return False
        self._file.seek(index * t.piece_length)
        self._file.write(data)
        self._file.flush()
        self._mark_done(index)
        return True
