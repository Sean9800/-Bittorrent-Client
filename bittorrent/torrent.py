"""Parsing of .torrent metainfo files (BEP 3)."""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from typing import List, Optional

from . import bencode


class TorrentError(ValueError):
    pass


@dataclass
class Torrent:
    name: str
    piece_length: int
    piece_hashes: List[bytes]
    length: int
    info_hash: bytes
    announce: Optional[str] = None
    announce_list: List[List[str]] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    @classmethod
    def from_file(cls, path: str) -> "Torrent":
        with open(path, "rb") as f:
            return cls.from_bytes(f.read())

    @classmethod
    def from_bytes(cls, data: bytes) -> "Torrent":
        try:
            meta = bencode.decode(data)
            info_raw = bencode.raw_value(data, b"info")
        except (bencode.BencodeError, KeyError) as e:
            raise TorrentError(f"not a valid .torrent file: {e}") from e
        if not isinstance(meta, dict) or not isinstance(meta.get(b"info"), dict):
            raise TorrentError("missing 'info' dictionary")
        info = meta[b"info"]

        if b"files" in info:
            raise NotImplementedError(
                "multi-file torrents are not supported yet (single-file only)"
            )
        try:
            name = os.path.basename(info[b"name"].decode("utf-8", "replace"))
            piece_length = int(info[b"piece length"])
            pieces = info[b"pieces"]
            length = int(info[b"length"])
        except (KeyError, AttributeError, TypeError) as e:
            raise TorrentError(f"missing or malformed info field: {e}") from e

        if not name or name in (".", ".."):
            raise TorrentError("invalid torrent name")
        if piece_length <= 0 or length < 0:
            raise TorrentError("invalid piece length / file length")
        if len(pieces) % 20 != 0:
            raise TorrentError("'pieces' length is not a multiple of 20")
        hashes = [pieces[i:i + 20] for i in range(0, len(pieces), 20)]
        if len(hashes) != -(-length // piece_length):
            raise TorrentError("piece count does not match file length")

        announce = meta.get(b"announce")
        announce = announce.decode("utf-8", "replace") if announce else None
        tiers: List[List[str]] = []
        for tier in meta.get(b"announce-list", []) or []:
            urls = [u.decode("utf-8", "replace") for u in tier if isinstance(u, bytes)]
            if urls:
                tiers.append(urls)
        if announce and not any(announce in t for t in tiers):
            tiers.insert(0, [announce])

        return cls(
            name=name,
            piece_length=piece_length,
            piece_hashes=hashes,
            length=length,
            info_hash=hashlib.sha1(info_raw).digest(),
            announce=announce,
            announce_list=tiers,
        )

    # ------------------------------------------------------------------ #
    @property
    def num_pieces(self) -> int:
        return len(self.piece_hashes)

    def piece_size(self, index: int) -> int:
        """Size of piece `index` (the last piece is usually shorter)."""
        if index == self.num_pieces - 1:
            return self.length - index * self.piece_length
        return self.piece_length


def build_torrent_bytes(data: bytes, name: str, piece_length: int = 262144,
                        announce: Optional[str] = None) -> bytes:
    """Create a single-file .torrent from in-memory data (handy for tests)."""
    pieces = b"".join(
        hashlib.sha1(data[i:i + piece_length]).digest()
        for i in range(0, len(data), piece_length)
    )
    meta = {
        "info": {
            "name": name,
            "length": len(data),
            "piece length": piece_length,
            "pieces": pieces,
        }
    }
    if announce:
        meta["announce"] = announce
    return bencode.encode(meta)
