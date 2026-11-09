"""Offline demo: seeds a random file on localhost and downloads it with the CLI.

    python examples/local_demo.py
"""
import asyncio, hashlib, os, random, sys, tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from bittorrent.cli import main
from bittorrent.torrent import Torrent, build_torrent_bytes
from tests.seeder import Seeder


async def demo():
    data = random.Random(7).randbytes(3_000_000)
    raw = build_torrent_bytes(data, "demo.bin", piece_length=262144)
    tmp = tempfile.mkdtemp()
    torrent_path = os.path.join(tmp, "demo.torrent")
    open(torrent_path, "wb").write(raw)

    t = Torrent.from_bytes(raw)
    seeder = Seeder(data, t.info_hash, t.piece_length)
    port = await seeder.start()
    print(f"seeder on 127.0.0.1:{port}, torrent at {torrent_path}")

    out = os.path.join(tmp, "out")
    rc = await asyncio.to_thread(main, [torrent_path, "-o", out, "--peer", f"127.0.0.1:{port}"])
    ok = hashlib.sha1(open(os.path.join(out, "demo.bin"), "rb").read()).digest() == hashlib.sha1(data).digest()
    print("exit code:", rc, "| file matches original:", ok)
    await seeder.stop()


asyncio.run(demo())
