from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import AsyncIterator, Iterable

from astream.stream import sink, stream
from astream.stream_utils import arange
from astream.utils import iter_to_aiter

try:
    from zstandard import ZstdCompressor, ZstdDecompressor
except ImportError:
    raise ImportError("zstd integration requires the zstandard library.")


@sink
async def to_zstd_file(
    async_iterator: AsyncIterator[bytes],
    output_file: Path | str,
    chunk_size: int = 1024 * 1024 * 2,
) -> int:

    output_file = Path(output_file)
    compressor = ZstdCompressor()
    buf: list[bytes] = []
    buf_len = 0
    written = 0

    with compressor.stream_writer(output_file.open("wb"), closefd=True) as writer:
        async for chunk in async_iterator:
            buf.append(chunk)
            buf_len += len(chunk)
            if buf_len >= chunk_size:
                written += await asyncio.to_thread(writer.write, b"".join(buf))
                buf.clear()
                buf_len = 0
        if buf:
            written += await asyncio.to_thread(writer.write, b"".join(buf))

    return written


@stream
def from_zstd_file(
    input_file: str | Path,
    chunk_size: int = 1024 * 1024 * 2,
) -> AsyncIterator[bytes]:

    _input_path = Path(input_file)
    decompressor = ZstdDecompressor()

    def _reader() -> Iterable[bytes]:
        with decompressor.stream_reader(_input_path.open("rb"), closefd=True) as reader:
            while True:
                data = reader.read(chunk_size)
                if not data:
                    break
                yield data

    return iter_to_aiter(_reader())


if __name__ == "__main__":

    async def main() -> None:

        tmp_file = Path(tempfile.NamedTemporaryFile("rb+").name)

        written = await (
            # arange(1000000)
            arange(100000)
            / (lambda i: f"Hi! This is line number {i}.\n".encode())
            / to_zstd_file(tmp_file)
        )
        # reveal_type(written)  # int
        print(f"Wrote {written} bytes")

        read = bytearray()
        async for chnk in from_zstd_file(tmp_file):
            read.extend(chnk)
            # reveal_type(chnk)  # Revealed type is "builtins.bytes"

        print(f"Read {len(read)}")

    asyncio.run(main())


__all__ = ("from_zstd_file", "to_zstd_file")