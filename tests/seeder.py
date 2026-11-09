"""A tiny in-process BitTorrent seeder used to test the client offline."""
import asyncio
import struct

PSTR = b"BitTorrent protocol"


class Seeder:
    def __init__(self, data, info_hash, piece_length, *, have=None,
                 corrupt_once=(), choke_after=None):
        self.data, self.info_hash, self.pl = data, info_hash, piece_length
        self.num_pieces = -(-len(data) // piece_length)
        self.have = set(range(self.num_pieces)) if have is None else set(have)
        self.corrupt_once = set(corrupt_once)
        self.choke_after = choke_after
        self.requests = []          # (piece, begin) log
        self.server = None
        self.port = None

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()

    def _bitfield(self):
        b = bytearray((self.num_pieces + 7) // 8)
        for i in self.have:
            b[i >> 3] |= 0x80 >> (i & 7)
        return bytes(b)

    @staticmethod
    def _msg(mid, payload=b""):
        return struct.pack(">IB", 1 + len(payload), mid) + payload

    async def _handle(self, reader, writer):
        try:
            hs = await reader.readexactly(68)
            if hs[28:48] != self.info_hash:
                return
            writer.write(bytes([19]) + PSTR + bytes(8) + self.info_hash + b"-SD0001-000000000000")
            writer.write(self._msg(5, self._bitfield()))
            await writer.drain()
            choked, served, choked_once = True, 0, False
            loop = asyncio.get_running_loop()

            async def unchoke():
                nonlocal choked
                choked = False
                writer.write(self._msg(1))
                await writer.drain()

            while True:
                (length,) = struct.unpack(">I", await reader.readexactly(4))
                if length == 0:
                    continue
                payload = await reader.readexactly(length)
                mid, body = payload[0], payload[1:]
                if mid == 2:  # interested
                    await unchoke()
                elif mid == 6 and not choked:
                    idx, begin, ln = struct.unpack(">III", body)
                    if idx not in self.have:
                        continue
                    self.requests.append((idx, begin))
                    block = bytearray(self.data[idx * self.pl + begin: idx * self.pl + begin + ln])
                    if begin == 0 and idx in self.corrupt_once:
                        self.corrupt_once.discard(idx)
                        block[0] ^= 0xFF
                    writer.write(self._msg(7, struct.pack(">II", idx, begin) + bytes(block)))
                    await writer.drain()
                    served += 1
                    if self.choke_after and served == self.choke_after and not choked_once:
                        choked_once, choked = True, True
                        writer.write(self._msg(0))  # CHOKE; requests now dropped
                        await writer.drain()
                        loop.call_later(0.2, lambda: asyncio.ensure_future(unchoke()))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
