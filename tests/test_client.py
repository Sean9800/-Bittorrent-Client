import asyncio
import hashlib
import os
import random
import socket
import struct
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qsl, urlparse

from bittorrent import bencode
from bittorrent.client import DownloadError, TorrentClient
from bittorrent.torrent import Torrent, TorrentError, build_torrent_bytes
from bittorrent.tracker import parse_compact_peers
from tests.seeder import Seeder

PL = 65536
DATA = random.Random(1).randbytes(PL * 6 + 5000)  # 7 pieces, last one short


def make_torrent(announce=None, data=DATA):
    return Torrent.from_bytes(build_torrent_bytes(data, "file.bin", PL, announce))


def compact(port):
    return socket.inet_aton("127.0.0.1") + struct.pack(">H", port)


class BencodeTests(unittest.TestCase):
    def test_roundtrip(self):
        obj = {b"a": [1, -5, 0, b"xyz"], b"b": {b"c": b""}}
        self.assertEqual(bencode.decode(bencode.encode(obj)), obj)

    def test_known_encoding(self):
        self.assertEqual(bencode.encode({"cow": "moo", "spam": "eggs"}),
                         b"d3:cow3:moo4:spam4:eggse")

    def test_rejects_garbage(self):
        for bad in (b"i03e", b"i-0e", b"5:abc", b"l", b"d1:ae", b"i1e extra", b"", b"x"):
            with self.assertRaises(bencode.BencodeError, msg=bad):
                bencode.decode(bad)


class TorrentTests(unittest.TestCase):
    def test_parse(self):
        t = make_torrent("http://t/announce")
        self.assertEqual((t.name, t.length, t.num_pieces), ("file.bin", len(DATA), 7))
        self.assertEqual(t.piece_size(6), 5000)
        self.assertEqual(t.announce_list, [["http://t/announce"]])

    def test_info_hash_uses_original_bytes(self):
        raw = build_torrent_bytes(DATA, "file.bin", PL)
        info = bencode.raw_value(raw, b"info")
        self.assertEqual(Torrent.from_bytes(raw).info_hash, hashlib.sha1(info).digest())

    def test_multifile_rejected(self):
        meta = bencode.encode({"info": {"name": "d", "piece length": 4, "pieces": b"",
                                        "files": [{"length": 1, "path": ["a"]}]}})
        with self.assertRaises(NotImplementedError):
            Torrent.from_bytes(meta)

    def test_bad_piece_count(self):
        meta = bencode.encode({"info": {"name": "x", "length": 100, "piece length": 10,
                                        "pieces": b"\0" * 20}})
        with self.assertRaises(TorrentError):
            Torrent.from_bytes(meta)

    def test_compact_peers(self):
        self.assertEqual(parse_compact_peers(compact(6881) + compact(80)),
                         [("127.0.0.1", 6881), ("127.0.0.1", 80)])


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = os.path.join(self.tmp.name, "file.bin")
        self.t = make_torrent()

    def tearDown(self):
        self.tmp.cleanup()

    async def seeder(self, **kw):
        s = Seeder(DATA, self.t.info_hash, PL, **kw)
        await s.start()
        self.addCleanup(lambda: None)
        return s

    async def download(self, peers, torrent=None, timeout=20):
        c = TorrentClient(torrent or self.t, self.out, extra_peers=peers, progress=False)
        await asyncio.wait_for(c.run(), timeout)
        with open(self.out, "rb") as f:
            return f.read()

    async def test_single_seeder(self):
        s = await self.seeder()
        try:
            self.assertEqual(await self.download([("127.0.0.1", s.port)]), DATA)
        finally:
            await s.stop()

    async def test_corrupt_piece_is_redownloaded(self):
        s = await self.seeder(corrupt_once={0, 3})
        try:
            self.assertEqual(await self.download([("127.0.0.1", s.port)]), DATA)
            self.assertGreater(len([r for r in s.requests if r == (0, 0)]), 1)
        finally:
            await s.stop()

    async def test_survives_choke_mid_piece(self):
        s = await self.seeder(choke_after=6)
        try:
            self.assertEqual(await self.download([("127.0.0.1", s.port)]), DATA)
        finally:
            await s.stop()

    async def test_pieces_split_across_peers(self):
        a = await self.seeder(have={0, 1, 2, 3})
        b = await self.seeder(have={4, 5, 6})
        try:
            got = await self.download([("127.0.0.1", a.port), ("127.0.0.1", b.port)])
            self.assertEqual(got, DATA)
            self.assertTrue(a.requests and b.requests)
        finally:
            await a.stop(); await b.stop()

    async def test_resume_only_fetches_missing(self):
        s = await self.seeder()
        try:
            await self.download([("127.0.0.1", s.port)])
            s.requests.clear()
            with open(self.out, "r+b") as f:       # wipe piece 2
                f.seek(2 * PL); f.write(bytes(PL))
            self.assertEqual(await self.download([("127.0.0.1", s.port)]), DATA)
            self.assertEqual({p for p, _ in s.requests}, {2})
        finally:
            await s.stop()

    async def test_no_peers_fails_cleanly(self):
        with self.assertRaises(DownloadError):
            await self.download([("127.0.0.1", 1)])  # nothing listening

    async def test_http_tracker(self):
        s = await self.seeder()
        seen = {}

        class H(BaseHTTPRequestHandler):
            def do_GET(h):
                q = dict(parse_qsl(urlparse(h.path).query, encoding="latin-1"))
                seen.setdefault("info_hash", q["info_hash"].encode("latin-1"))
                seen.setdefault("events", []).append(q.get("event"))
                body = bencode.encode({"interval": 60, "peers": compact(s.port)})
                h.send_response(200); h.end_headers(); h.wfile.write(body)
            def log_message(*a): pass

        srv = HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            t = make_torrent(f"http://127.0.0.1:{srv.server_port}/announce")
            self.assertEqual(await self.download([], torrent=t), DATA)
            self.assertEqual(seen["info_hash"], t.info_hash)
            self.assertEqual(seen["events"][0], "started")
            self.assertIn("completed", seen["events"])
        finally:
            srv.shutdown(); srv.server_close(); await s.stop()

    async def test_udp_tracker(self):
        s = await self.seeder()
        t = None

        class Proto(asyncio.DatagramProtocol):
            def connection_made(p, transport): p.tr = transport
            def datagram_received(p, data, addr):
                if len(data) == 16:
                    magic, action, tx = struct.unpack(">QII", data)
                    assert magic == 0x41727101980 and action == 0
                    p.tr.sendto(struct.pack(">IIQ", 0, tx, 0xABCDEF), addr)
                elif len(data) == 98:
                    conn, action, tx = struct.unpack(">QII", data[:16])
                    assert conn == 0xABCDEF and action == 1
                    assert data[16:36] == t.info_hash
                    p.tr.sendto(struct.pack(">IIIII", 1, tx, 60, 0, 1) + compact(s.port), addr)

        loop = asyncio.get_running_loop()
        tr, _ = await loop.create_datagram_endpoint(Proto, local_addr=("127.0.0.1", 0))
        try:
            t = make_torrent(f"udp://127.0.0.1:{tr.get_extra_info('sockname')[1]}/announce")
            self.assertEqual(await self.download([], torrent=t), DATA)
        finally:
            tr.close(); await s.stop()


if __name__ == "__main__":
    unittest.main()
