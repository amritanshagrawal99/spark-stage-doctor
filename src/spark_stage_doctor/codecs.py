"""Decompress Spark event logs.

Spark has no codec header: it names the file `<app>.<codec>`. Databricks delivers gzip. Compression is recognised by
the file name and, failing that, by the first bytes. Spark's lz4 and snappy are the JAVA BLOCK formats written by
lz4-java (LZ4BlockOutputStream) and snappy-java (SnappyOutputStream). They are not the standard LZ4 frame or Snappy
framing formats, so generic Python readers can't decode them. They are parsed here by hand.

    zstd    zstd frames (zstandard)            magic 28 b5 2f fd
    gzip    gzip                               magic 1f 8b
    lz4     "LZ4Block" + token + 3 LE ints     magic b"LZ4Block"
    snappy  xerial: 16-byte header, then (BE int length, raw snappy chunk)*   magic 82 "SNAPPY" 00
    lzf     ning LZF chunks "ZV" + type        magic b"ZV"

A log that is still being written (`.inprogress`) can end mid-block. Every reader returns what it could decode and
reports the truncation instead of failing.
"""
from __future__ import annotations

import struct
import zlib
from collections.abc import Iterator
from pathlib import Path

CODECS = ("zstd", "gzip", "lz4", "snappy", "lzf", "plain")
_EXT = {"zstd": "zstd", "zst": "zstd", "gz": "gzip", "lz4": "lz4", "snappy": "snappy", "lzf": "lzf"}
LZ4_MAGIC = b"LZ4Block"
SNAPPY_MAGIC = b"\x82SNAPPY\x00"


class Truncated(Exception):
    """The compressed stream ended mid-block (a log still being written). Carries the bytes decoded so far."""

    def __init__(self, data: bytes, why: str):
        super().__init__(why)
        self.data = data


def codec_for(path: str | Path, head: bytes) -> str:
    name = Path(path).name
    for suffix in (".inprogress", ".compact"):
        name = name.removesuffix(suffix)
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext in _EXT:
        return _EXT[ext]
    if head.startswith(b"\x28\xb5\x2f\xfd"):
        return "zstd"
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    if head.startswith(LZ4_MAGIC):
        return "lz4"
    if head.startswith(SNAPPY_MAGIC):
        return "snappy"
    if head.startswith(b"ZV") and len(head) > 2 and head[2] in (0, 1):
        return "lzf"
    return "plain"


def decompress(raw: bytes, codec: str) -> bytes:
    """Whole-buffer decode. Raises Truncated (with the decoded prefix) if the stream stops mid-block."""
    if codec == "plain":
        return raw
    if codec == "gzip":
        return _gzip(raw)
    if codec == "zstd":
        return _zstd(raw)
    if codec == "lz4":
        return _lz4_block_stream(raw)
    if codec == "snappy":
        return _xerial_snappy(raw)
    if codec == "lzf":
        return _lzf_stream(raw)
    raise ValueError(f"unknown codec {codec!r}")


# ------------------------------------------------------------------ gzip / zstd

def _gzip(raw: bytes) -> bytes:
    out = bytearray()
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        data = raw
        while data:
            out += d.decompress(data)
            data = d.unused_data            # concatenated gzip members
            if data:
                d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        if not d.eof:
            raise Truncated(bytes(out), "gzip stream ended before its trailer")
    except zlib.error as e:
        raise Truncated(bytes(out), f"gzip: {e}") from None
    return bytes(out)


def _zstd(raw: bytes) -> bytes:
    """Frame by frame, so an unfinished last frame is detected (a stream reader just stops at it silently)."""
    import zstandard

    out = bytearray()
    data = raw
    try:
        while data:
            d = zstandard.ZstdDecompressor().decompressobj()
            out += d.decompress(data)
            if not d.eof:
                raise Truncated(bytes(out), "zstd stream ended mid-frame")
            data = d.unused_data
    except zstandard.ZstdError as e:
        raise Truncated(bytes(out), f"zstd: {e}") from None
    return bytes(out)


# ------------------------------------------------------------------ lz4-java block stream

def _lz4_block_stream(raw: bytes) -> bytes:
    """lz4-java LZ4BlockOutputStream: per block, 8-byte magic, 1 token byte (method in the high nibble: 0x10 raw,
    0x20 lz4), then little-endian compressed length, decompressed length and checksum, then the payload. A block
    with decompressed length 0 ends the stream."""
    import cramjam

    out = bytearray()
    pos, n = 0, len(raw)
    while pos < n:
        if n - pos < 21:
            raise Truncated(bytes(out), "lz4: block header cut off")
        if raw[pos:pos + 8] != LZ4_MAGIC:
            raise ValueError(f"lz4: bad block magic at byte {pos}")
        token = raw[pos + 8]
        clen, dlen, _checksum = struct.unpack_from("<iii", raw, pos + 9)
        pos += 21
        if dlen == 0:
            break                                       # end-of-stream marker
        if pos + clen > n:
            raise Truncated(bytes(out), "lz4: block payload cut off")
        payload = raw[pos:pos + clen]
        pos += clen
        method = token & 0xF0
        if method == 0x10:
            out += payload
        elif method == 0x20:
            out += bytes(cramjam.lz4.decompress_block(payload, output_len=dlen))
        else:
            raise ValueError(f"lz4: unknown compression method {method:#x}")
    return bytes(out)


# ------------------------------------------------------------------ xerial snappy stream

def _xerial_snappy(raw: bytes) -> bytes:
    """snappy-java SnappyOutputStream: 8-byte magic, BE int version, BE int compatible version, then chunks of
    (BE int compressed length, raw snappy block)."""
    import cramjam

    if not raw.startswith(SNAPPY_MAGIC):
        raise ValueError("snappy: missing xerial header")
    out = bytearray()
    pos, n = 16, len(raw)
    while pos < n:
        if n - pos < 4:
            raise Truncated(bytes(out), "snappy: chunk length cut off")
        (clen,) = struct.unpack_from(">i", raw, pos)
        pos += 4
        if pos + clen > n:
            raise Truncated(bytes(out), "snappy: chunk cut off")
        out += bytes(cramjam.snappy.decompress_raw(raw[pos:pos + clen]))
        pos += clen
    return bytes(out)


# ------------------------------------------------------------------ ning LZF

def _lzf_stream(raw: bytes) -> bytes:
    """ning compress-lzf chunks: "ZV", type (0 = stored, 1 = compressed), BE u16 length(s), payload."""
    out = bytearray()
    pos, n = 0, len(raw)
    while pos < n:
        if n - pos < 5 or raw[pos:pos + 2] != b"ZV":
            if n - pos < 5:
                raise Truncated(bytes(out), "lzf: chunk header cut off")
            raise ValueError(f"lzf: bad chunk magic at byte {pos}")
        kind = raw[pos + 2]
        if kind == 0:
            (length,) = struct.unpack_from(">H", raw, pos + 3)
            pos += 5
            if pos + length > n:
                raise Truncated(bytes(out), "lzf: stored chunk cut off")
            out += raw[pos:pos + length]
            pos += length
        elif kind == 1:
            if n - pos < 7:
                raise Truncated(bytes(out), "lzf: chunk header cut off")
            clen, dlen = struct.unpack_from(">HH", raw, pos + 3)
            pos += 7
            if pos + clen > n:
                raise Truncated(bytes(out), "lzf: compressed chunk cut off")
            chunk = _lzf_block(raw[pos:pos + clen], dlen)
            out += chunk
            pos += clen
        else:
            raise ValueError(f"lzf: unknown chunk type {kind}")
    return bytes(out)


def _lzf_block(src: bytes, expected: int) -> bytes:
    out = bytearray()
    i, n = 0, len(src)
    while i < n:
        ctrl = src[i]
        i += 1
        if ctrl < 32:                         # literal run of ctrl + 1 bytes
            out += src[i:i + ctrl + 1]
            i += ctrl + 1
            continue
        length = ctrl >> 5                    # back reference
        if length == 7:
            length += src[i]
            i += 1
        ref = len(out) - ((ctrl & 0x1F) << 8) - src[i] - 1
        i += 1
        if ref < 0:
            raise ValueError("lzf: back reference before the start of the block")
        for _ in range(length + 2):           # byte by byte: the copy may overlap its own output
            out.append(out[ref])
            ref += 1
    if len(out) != expected:
        raise ValueError(f"lzf: block decoded to {len(out)} bytes, header says {expected}")
    return bytes(out)


def read_lines(path: Path) -> tuple[Iterator[str], str, str | None]:
    """(lines, codec, truncation note or None) for one event-log file."""
    raw = path.read_bytes()
    codec = codec_for(path, raw[:16])
    note = None
    try:
        data = decompress(raw, codec)
    except Truncated as t:
        data, note = t.data, f"{path.name}: {t}"
    text = data.decode("utf-8", errors="replace")
    return iter(text.splitlines()), codec, note


__all__ = ["CODECS", "Truncated", "codec_for", "decompress", "read_lines"]
