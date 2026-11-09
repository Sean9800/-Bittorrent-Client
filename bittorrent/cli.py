from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from .client import DownloadError, TorrentClient, human_size
from .torrent import Torrent, TorrentError


def parse_peer(s: str):
    host, _, port = s.rpartition(":")
    if not host or not port.isdigit():
        raise argparse.ArgumentTypeError("peer must look like host:port")
    return host.strip("[]"), int(port)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="bittorrent",
                                 description="Minimal BitTorrent downloader (single-file torrents)")
    ap.add_argument("torrent", help="path to a .torrent file")
    ap.add_argument("-o", "--output", default=".", help="output directory (default: .)")
    ap.add_argument("--peer", action="append", type=parse_peer, default=[],
                    metavar="HOST:PORT", help="connect directly to a peer (repeatable)")
    ap.add_argument("--max-peers", type=int, default=30)
    ap.add_argument("--port", type=int, default=6881, help="port reported to trackers")
    ap.add_argument("--info", action="store_true", help="print torrent info and exit")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=[logging.WARNING, logging.INFO, logging.DEBUG][min(args.verbose, 2)],
        format="%(levelname)s %(name)s: %(message)s")

    try:
        t = Torrent.from_file(args.torrent)
    except (OSError, TorrentError, NotImplementedError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.info:
        print(f"name:         {t.name}")
        print(f"size:         {human_size(t.length)} ({t.length} bytes)")
        print(f"piece length: {human_size(t.piece_length)}")
        print(f"pieces:       {t.num_pieces}")
        print(f"info hash:    {t.info_hash.hex()}")
        for i, tier in enumerate(t.announce_list):
            print(f"tracker tier {i}: {', '.join(tier)}")
        return 0

    out = os.path.join(args.output, t.name)
    client = TorrentClient(t, out, port=args.port, max_peers=args.max_peers,
                           extra_peers=args.peer)
    print(f"Downloading {t.name} ({human_size(t.length)}) -> {out}", file=sys.stderr)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\ninterrupted - progress is saved, re-run to resume", file=sys.stderr)
        return 130
    except DownloadError as e:
        print(f"\nerror: {e}", file=sys.stderr)
        return 1
    print(f"Done: {out}", file=sys.stderr)
    return 0