from __future__ import annotations

import asyncio
import os
import re
from asyncio import Task, StreamReader
from asyncio.subprocess import Process
from dataclasses import dataclass
from functools import partialmethod, partial
from operator import itemgetter
from pathlib import Path
from typing import (
    Sequence,
    Protocol,
    cast,
    Literal,
    TypeGuard,
    Any,
    AsyncIterator,
    AsyncIterable,
    Coroutine,
)

from loguru import logger

from astream import Stream, amerge, with_exc_handler, NoValueT, NoValue
from astream import bytes_stream_split_separator
from astream.experimental.partializer import F
from astream.protocols import StreamLike


class ProcessWithFds(Process):

    stdout: asyncio.StreamReader
    stderr: asyncio.StreamReader
    stdin: asyncio.StreamWriter

    @classmethod
    def validate_process(cls, process: Process | None) -> TypeGuard[ProcessWithFds]:
        return (
            isinstance(process, Process)
            and getattr(process, "stdout", None) is not None
            and getattr(process, "stderr", None) is not None
            and getattr(process, "stdin", None) is not None
        )


class Proc(StreamLike[bytes], AsyncIterable[bytes]):
    _proc: ProcessWithFds | None = None
    _merged_streams: Stream[bytes]

    def __init__(self, proc: ProcessWithFds, cmd: str, cmd_args: tuple[str, ...]) -> None:
        self.proc = proc
        self.cmd = cmd
        self.cmd_args = cmd_args

        self._feeders: set[asyncio.Task[None]] = set()
        self._merged_streams = Stream(self._do_merge_streams())

    async def _do_merge_streams(self) -> AsyncIterator[bytes]:
        merged = amerge(self.stdout, self.stderr)
        while True:
            try:
                yield await anext(merged)
            except asyncio.IncompleteReadError as exc:
                yield exc.partial
            except asyncio.LimitOverrunError:
                continue
            except StopAsyncIteration:
                break

    @property
    def stdin(self) -> asyncio.StreamWriter:
        return self.proc.stdin

    @property
    def stdout(self) -> Stream[str]:
        return Stream(self.proc.stdout) / bytes.strip / bytes.decode

    @property
    def stderr(self) -> Stream[str]:
        return Stream(self.proc.stderr) / bytes.strip / bytes.decode

    async def _raw_reader(self, stream: StreamReader) -> AsyncIterator[bytes]:
        while True:
            try:
                read = await stream.read(4096)
                if not read:
                    break
            except asyncio.IncompleteReadError as exc:
                yield exc.partial
            except asyncio.LimitOverrunError as exc:
                print("LimitOverrunError", exc)
                continue

    @property
    def stdout_raw(self) -> Stream[bytes]:
        return Stream(self._raw_reader(self.proc.stdout))

    @property
    def stderr_raw(self) -> Stream[bytes]:
        return Stream(self._raw_reader(self.proc.stderr))

    stdout_with_separator = partialmethod(bytes_stream_split_separator)
    stderr_with_separator = partialmethod(bytes_stream_split_separator)

    @property
    def merged_streams(self) -> Stream[bytes]:
        """Return lines from both stdout and stderr."""
        return self._merged_streams

    def __iter__(self) -> tuple[Stream[str], Stream[str]]:
        """Return stdout and stderr streams, to enable `stdout, stderr = Proc(...)`"""
        return self.stdout, self.stderr

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Return merged streams, with lines from both stdout and stderr."""
        return self.merged_streams

    @classmethod
    async def run_exec(cls, cmd: str, *cmd_args: str, **subprocess_exec_kwargs: Any) -> Proc:
        """Create a Proc instance from a command and arguments."""
        proc = await asyncio.create_subprocess_exec(
            cmd,
            *cmd_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            **subprocess_exec_kwargs,
        )
        if ProcessWithFds.validate_process(proc):
            validated_proc = cast(ProcessWithFds, proc)  # type: ignore
            return cls(validated_proc, cmd, cmd_args)
        raise TypeError(f"Expected {ProcessWithFds}, got {proc}")

    @classmethod
    async def run_shell(cls, cmd: str, **subprocess_exec_kwargs: Any) -> Proc:
        """Create a Proc instance from a shell command."""
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            **subprocess_exec_kwargs,
        )
        if ProcessWithFds.validate_process(proc):
            validated_proc = cast(ProcessWithFds, proc)  # type: ignore
            return cls(validated_proc, cmd, ())
        raise TypeError(f"Expected {ProcessWithFds}, got {proc}")

    def feed_from(
        self,
        stream: AsyncIterable[bytes] | AsyncIterable[str],
        separator: bytes | None = b"\n",
    ) -> Coroutine[Any, Any, None]:
        """Feed the process with data from a stream."""

        async def _feeder() -> None:
            async for data in stream:
                if isinstance(data, str):
                    data = data.encode()
                if separator is not None and not data.endswith(separator):
                    data += separator
                self.stdin.write(data)
            await self.stdin.drain()
            self.stdin.close()

        return _feeder()

    def __rrshift__(self, other: AsyncIterable[bytes] | AsyncIterable[str]) -> Proc:

        if isinstance(other, Proc):
            other = other.stdout

        task = asyncio.create_task(self.feed_from(other))
        self._feeders.add(task)
        task.add_done_callback(self._feeders.remove)
        return self

    def __rshift__(self, other: AsyncIterable[bytes] | AsyncIterable[str]) -> Proc:
        # todo - double check this -- can't be right for both rshift and rrshift to be the same

        if isinstance(other, Proc):
            other = other.stdout

        task = asyncio.create_task(self.feed_from(other))
        self._feeders.add(task)
        task.add_done_callback(self._feeders.remove)
        return self

    __lshift__ = __rrshift__
    __rlshift__ = __rshift__

    def __str__(self) -> str:
        return f"Proc({self.cmd} {self.cmd_args})"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class JpegRecompressOutput:
    """Output from jpeg-recompress."""

    input_path: Path
    output_path: Path
    recompressed: bool

    original_size: int
    recompressed_size: int | None = None
    metadata_size: int | None = None

    final_quality: int | None = None
    final_ssim: float | None = None


@dataclass
class JpegRecompress:

    input_path: Path
    output_path: Path
    quality: Literal["low", "medium", "high", "veryhigh"] = "high"
    min_quality: int = 80
    max_quality: int = 100
    method: str = "ssim"

    @property
    def as_args(self) -> list[str]:
        return [
            "--quality",
            str(self.quality),
            "--min",
            str(self.min_quality),
            "--max",
            str(self.max_quality),
            "--method",
            self.method,
            str(self.input_path),
            str(self.output_path),
        ]

    @classmethod
    async def apply(cls, input_file: Path, output_file: Path) -> JpegRecompressOutput:
        """Apply jpeg-recompress to a file."""
        proc = await Proc.run_exec("jpeg-recompress", *cls(input_file, output_file).as_args)
        props = {
            "input_path": input_file,
            "output_path": output_file,
            "original_size": input_file.stat().st_size,
            "recompressed": False,
        }

        async for line in proc.stderr_decoded:
            print(line)
            if m := re.search(r"Final optimized ssim at q=(?P<q>\d+): (?P<ssim>\d+\.\d+)", line):
                props["final_ssim"] = float(m.group("ssim"))
                props["final_quality"] = int(m.group("q"))
            elif m := re.search(r"Metadata size is (?P<size_kb>\d+)kb", line):
                props["metadata_size"] = int(m.group("size_kb")) * 1024
            elif "New size is" in line:
                props["recompressed"] = True
            elif "File already processed" in line:
                props["recompressed"] = False
            else:
                print("no matches on", line)

        props["recompressed_size"] = output_file.stat().st_size
        return JpegRecompressOutput(**props)


if __name__ == "__main__":

    async def main() -> None:
        proc = await Proc.run_shell("ls -l")
        async for line in proc:
            print(line)

        proc_du = await Proc.run_shell("du")
        # total_size = await (proc_du / bytes.split / itemgetter(0) / int @ sum)
        # print(total_size)

        fd = await Proc.run_exec("fd", "-tx", "-tf", ".", *os.environ.get("PATH", "").split(":"))
        cat = await Proc.run_exec("cat", "-A")
        proc_rg = await Proc.run_exec("rg", "bin")
        a = fd >> cat
        proc_rg << a
        print(a)
        async for line in proc_rg.stdout:
            print(line)

        out = await JpegRecompress.apply(
            input_file=Path.home() / "in.jpg",
            output_file=Path.home() / "out.jpg",
        )

        ims = await Proc.run_shell("fd -tf -e jpg . /home/pedro/projs/wp_dumper/bb_dumper/images")
        st = ims.stdout / Path / (lambda p: (p, p.with_name("optimized_" + p.name)))
        async for inp, out in st:
            res = await JpegRecompress.apply(inp, out)
            print(res)

    asyncio.run(main())
