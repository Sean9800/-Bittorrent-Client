# -Bittorrent-Client
This is  a BitTorrent client that can download files using the BitTorrent protocol. 
# pytorrent — a minimal BitTorrent client in Python

A from-scratch BitTorrent downloader for **single-file torrents**, built on
`asyncio` with **zero third-party dependencies** (Python 3.9+; developed on 3.12).

    python -m bittorrent ubuntu.iso.torrent -o ~/Downloads
    python -m bittorrent some.torrent --info          # inspect metadata
    python -m bittorrent some.torrent -v              # -v info, -vv debug
    python -m bittorrent some.torrent --peer 10.0.0.5:6881   # skip the tracker
    python examples/local_demo.py                     # fully offline demo
    python -m unittest discover -v                    # run the tests

Interrupting (Ctrl-C) is safe: on the next run existing pieces are re-hashed and
only missing ones are fetched.

## How BitTorrent works (and where it lives in the code)

| Step | What happens | File |
|------|--------------|------|
| 1. Parse | `.torrent` is *bencoded* metadata: name, length, piece size, SHA-1 of every piece, tracker URLs. | `bencode.py`, `torrent.py` |
| 2. Identify | The **info-hash** = SHA-1 of the raw bencoded `info` dict. It names the swarm. We hash the *original bytes* rather than re-encoding. | `bencode.raw_value` |
| 3. Find peers | Ask the tracker (HTTP compact format, or UDP per BEP 15) for a list of `ip:port`. Tiers from `announce-list` are tried in order. | `tracker.py` |
| 4. Talk to peers | TCP handshake (`\x13BitTorrent protocol` + info-hash + peer-id), then length-prefixed messages: `choke/unchoke/interested/have/bitfield/request/piece`. | `peer.py` |
| 5. Download | The file is split into pieces, pieces into 16 KiB blocks. Each peer connection claims a piece, keeps 8 block requests in flight (pipelining), and reassembles it. | `peer.py` |
| 6. Verify | A finished piece is SHA-1 checked against the torrent; bad pieces are discarded and re-fetched (peers that send 3 are dropped). | `pieces.py` |
| 7. Coordinate | Tracker re-announces, a pool of up to 30 peer workers, progress display, `started/completed/stopped` events. | `client.py` |

### Design notes
* **Rarest-first** piece selection (`PieceManager.claim`) keeps rare pieces from
  disappearing; **endgame** mode lets idle peers duplicate the last in-flight pieces.
* **Choking**: when a peer chokes us, it drops our pending requests, so we re-queue
  them and re-request after the next unchoke (tested).
* A cancelled `StreamReader.readexactly()` leaves unread bytes buffered, which is
  why the idle-poll timeout in `peer.py` can't desync the stream.
* The test suite includes an in-process **seeder** plus fake **HTTP and UDP trackers**,
  so everything (corrupt data, choking, split swarms, resume, both tracker types)
  is tested with no internet.

## Limitations / next steps (good exercises)
1. **Multi-file torrents** — map the byte range of each piece onto `info["files"]`
   (`Torrent` raises `NotImplementedError` for now; `PieceManager` is the place to change).
2. **Uploading / seeding** — send `BITFIELD`/`HAVE`, answer `REQUEST`s, listen on `--port`,
   implement a choking algorithm (tit-for-tat + optimistic unchoke).
3. **Magnet links** — BEP 9/10 metadata exchange so you only need an info-hash.
4. **DHT** (BEP 5) and **PEX** (BEP 11) for trackerless peer discovery.
5. Message-stream encryption (MSE), IPv6 peers in `peers6` are parsed but untested,
   per-peer speed ranking, and bandwidth limits.

Only download content you have the right to download.
