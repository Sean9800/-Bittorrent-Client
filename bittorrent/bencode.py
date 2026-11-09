"""Bencode encoder/decoder (BEP 3).

Bencode has four types:
    integers      i42e
    byte strings  4:spam
    lists         l4:spami42ee
    dictionaries  d3:cow3:moo4:spam4:eggse   (keys are byte strings, sorted)
"""
from __future__ import annotations

import re
from typing import Any

MAX_DEPTH = 100
_INT_RE = re.compile(rb"^(0|-?[1-9][0-9]*)$")


class BencodeError(ValueError):
    pass


# --------------------------------------------------------------------------- #
# Encoding
# --------------------------------------------------------------------------- #
def encode(obj: Any) -> bytes:
    out = bytearray()
    _encode(obj, out)
    return bytes(out)


def _encode(obj: Any, out: bytearray) -> None:
    if isinstance(obj, bool):
        raise BencodeError("booleans cannot be bencoded")
    if isinstance(obj, int):
        out += b"i%de" % obj
    elif isinstance(obj, (bytes, bytearray)):
        out += b"%d:" % len(obj)
        out += obj
    elif isinstance(obj, str):
        _encode(obj.encode("utf-8"), out)
    elif isinstance(obj, (list, tuple)):
        out += b"l"
        for item in obj:
            _encode(item, out)
        out += b"e"
    elif isinstance(obj, dict):
        items = []
        for k, v in obj.items():
            if isinstance(k, str):
                k = k.encode("utf-8")
            if not isinstance(k, bytes):
                raise BencodeError("dict keys must be str or bytes")
            items.append((k, v))
        items.sort(key=lambda kv: kv[0])  # spec: keys sorted as raw strings
        out += b"d"
        for k, v in items:
            _encode(k, out)
            _encode(v, out)
        out += b"e"
    else:
        raise BencodeError(f"cannot bencode {type(obj).__name__}")


# --------------------------------------------------------------------------- #
# Decoding
# --------------------------------------------------------------------------- #
def decode(data: bytes) -> Any:
    """Decode a complete bencoded value. Trailing garbage is an error."""
    value, end = _decode(data, 0, 0)
    if end != len(data):
        raise BencodeError("trailing data after bencoded value")
    return value


def raw_value(data: bytes, key: bytes) -> bytes:
    """Return the *exact original bytes* of a top-level dict value.

    The info-hash is the SHA-1 of the bencoded `info` dictionary as it appears
    in the .torrent file. Re-encoding it could differ from the original if the
    file was not canonical, so we slice the original bytes instead.
    """
    if data[:1] != b"d":
        raise BencodeError("top-level value is not a dictionary")
    i = 1
    while i < len(data) and data[i:i + 1] != b"e":
        k, i = _decode(data, i, 1)
        start = i
        _, i = _decode(data, i, 1)
        if k == key:
            return data[start:i]
    raise KeyError(key)


def _decode(data: bytes, i: int, depth: int):
    if depth > MAX_DEPTH:
        raise BencodeError("nesting too deep")
    if i >= len(data):
        raise BencodeError("unexpected end of data")
    c = data[i]

    if c == 0x69:  # 'i'
        end = data.find(b"e", i + 1)
        if end == -1:
            raise BencodeError("unterminated integer")
        raw = data[i + 1:end]
        if not _INT_RE.match(raw):
            raise BencodeError(f"invalid integer {raw!r}")
        return int(raw), end + 1

    if c == 0x6C:  # 'l'
        i += 1
        items = []
        while True:
            if i >= len(data):
                raise BencodeError("unterminated list")
            if data[i] == 0x65:
                return items, i + 1
            v, i = _decode(data, i, depth + 1)
            items.append(v)

    if c == 0x64:  # 'd'
        i += 1
        result = {}
        while True:
            if i >= len(data):
                raise BencodeError("unterminated dict")
            if data[i] == 0x65:
                return result, i + 1
            k, i = _decode(data, i, depth + 1)
            if not isinstance(k, bytes):
                raise BencodeError("dict key is not a string")
            v, i = _decode(data, i, depth + 1)
            result[k] = v

    if 0x30 <= c <= 0x39:  # digit -> string
        colon = data.find(b":", i)
        if colon == -1:
            raise BencodeError("string length without ':'")
        raw = data[i:colon]
        if not raw.isdigit() or (len(raw) > 1 and raw[:1] == b"0"):
            raise BencodeError(f"invalid string length {raw!r}")
        start = colon + 1
        end = start + int(raw)
        if end > len(data):
            raise BencodeError("string runs past end of data")
        return data[start:end], end

    raise BencodeError(f"unexpected byte {data[i:i+1]!r} at offset {i}")
