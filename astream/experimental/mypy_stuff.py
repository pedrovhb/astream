from __future__ import annotations

import asyncio
import atexit
import re
import shlex
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import NamedTuple
import typing

from loguru import logger
from rich import box
from rich.align import Align
from rich.console import Console, RenderResult, ConsoleOptions, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.measure import Measurement
from rich.padding import Padding
from rich.panel import Panel
from rich.segment import Segment
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Table, Column
from rich.text import Text
from rich.tree import Tree
from watchfiles import awatch, PythonFilter

"""
note - lots of data can be found in e.g. 
/home/pedro/projs/astream/.mypy_cache/3.10/astream/stream_utils.data.json
"""

con = Console()

logger.remove()
logger.configure(
    handlers=[
        {
            "sink": RichHandler(console=con, rich_tracebacks=True, tracebacks_show_locals=True),
            "level": "INFO",
        }
    ]
)


"""
Mypy output example to be parsed:

astream/formal.py:122:5:123:11: error: Missing return statement  [empty-body]
astream/formal.py:122:5:123:11: note: If the method is meant to be abstract, use @abc.abstractmethod
astream/formal.py:479:5:483:25: error: Argument 1 of "__transform__" is incompatible with supertype "TransformableAsyncIterable"; supertype defines the argument type as "Transformer[_T, _U]"  [override]
astream/formal.py:479:5:483:25: note: This violates the Liskov substitution principle
astream/formal.py:479:5:483:25: note: See https://mypy.readthedocs.io/en/stable/common_issues.html#incompatible-overrides
astream/formal.py:517:55:517:58: error: Argument 1 to "__stream_transform__" of "StreamTransformer" has incompatible type "Stream[_T]"; expected "Stream[_T]"  [arg-type]
astream/formal.py:520:16:520:25: error: Incompatible return value type (got "Union[Stream[_U], Stream[_U]]", expected "Stream[_U]")  [return-value]
astream/formal.py:543:16:543:19: error: Incompatible return value type (got "Tuple[AsyncIterator[_T], AsyncIterator[_T]]", expected "Tuple[Stream[_T], Stream[_T]]")  [return-value]
astream/formal.py:612:12:612:39: error: Incompatible return value type (got "Tuple[int, List[_T]]", expected "str")  [return-value]
astream/formal.py:656:25:656:25: note: Revealed type is "builtins.int"

"""


class MypyLineType(str, Enum):
    ERROR = "error"
    NOTE = "note"

    def __eq__(self, other):
        return self is other or self.value == other


class MypyErrorType(str, Enum):
    ReturnValue = "return-value"
    Argument = "arg-type"
    Assignment = "assignment"


class IncompatibleTypes(NamedTuple):
    line: MypyOutputLine
    got: str
    expected: str

    @classmethod
    def from_line(cls, line: MypyOutputLine) -> IncompatibleTypes | None:
        if line.error_type == MypyErrorType.ReturnValue:
            m = re.search(r"got \"(.*)\", expected \"(.*)\"", line.message)
            if m:
                return cls(line, m.group(1), m.group(2))
            else:
                logger.warning(f"Could not parse return value type error: {line}")
        elif line.error_type == MypyErrorType.Argument:
            m = re.search(r"has incompatible type \"(.*)\"; expected \"(.*)\"", line.message)
            if m:
                got = m.group(1)
                expected = m.group(2)
                return cls(line, got, expected)
            else:
                logger.warning(f"Could not parse line {line}")
                return None
        elif line.error_type == MypyErrorType.Assignment:
            m = re.search(
                r"Incompatible types in assignment \(expression "
                r"has type \"(.*)\", variable has type \"(.*)\"\)",
                line.message,
            )
            if m:
                got = m.group(1)
                expected = m.group(2)
                return cls(line, got, expected)
            else:
                logger.warning(f"Could not parse line {line}")
                return None
        else:
            logger.error(f"Unknown error type: {line.error_type}")
        return None

    def _inner_panel(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment(f"File {self.line.file}: \n")
        syntax = Syntax.from_path(
            self.line.file,
            line_numbers=True,
            line_range=(self.line.start_line, self.line.end_line),
        )
        yield syntax
        yield Segment(
            f"\t\tExpected  {self.expected}\n",
        )
        yield Segment(
            f"\t\tGot       {self.got}\n",
        )

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield from self._inner_panel(console, options)
        # yield Panel(
        #     Text.assemble(self._inner_panel(console, options)),
        #     title="Incompatible types",
        #     title_align="left",
        #     border_style="red",
        # )


NUM_CONTEXT_LINES = 3


class AnnotationTreeNode:
    def __init__(self, label: str, full_expression: str, children: list[AnnotationTreeNode] = None):
        self.children = children or []
        self.label = label
        self.full_expression = full_expression

    @classmethod
    def from_string_annotation(cls, s: str) -> AnnotationTreeNode:
        if "[" not in s:
            return cls(s, s)

        matches = re.match(r"(?P<expression>(?P<indexer>.*?)\[(?P<indexed>.*)\])", s)
        if matches is None:
            raise ValueError(f"Could not parse {s}")

        expression = matches.group("expression")
        label = matches.group("indexer")
        children = matches.group("indexed")
        return cls(label, expression, [cls.from_string_annotation(c) for c in children.split(", ")])

    @property
    def tree(self) -> Tree:
        tree = Tree(Text(self.label, style="bold"))
        for child in self.children:
            tree.add(child)
        return tree

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield self.tree

    def __rich_measure__(self, console: Console, options: ConsoleOptions) -> Measurement:
        return Measurement.get(console, options, self.tree)


@dataclass
class MypyOutputLine:
    file: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    line_type: str
    message: str

    @property
    def error_type(self) -> str | None:
        # String in brackets at end of line
        if (m := re.match(r".*\[(.*)\]$", self.message.strip())) is not None:
            return m.group(1)
        return None

    @classmethod
    def from_line(cls, line: str) -> MypyOutputLine | None:
        try:
            file, start_line, start_col, end_line, end_col, line_type, message = line.split(":", 6)
        except ValueError:
            logger.error(f"Could not parse line: {line}")
            return None
        return cls(
            file=file,
            start_line=int(start_line),
            start_col=int(start_col),
            end_line=int(end_line),
            end_col=int(end_col),
            line_type=line_type.strip(),
            message=message.strip(),
        )

    def _get_snippet(self):
        file_lines = Path(self.file).read_text().splitlines()
        start_line = max(self.start_line - NUM_CONTEXT_LINES, 1)
        end_line = min(self.end_line + NUM_CONTEXT_LINES, len(file_lines))

        snippet = file_lines[start_line - 1 : end_line]
        blank_lines_start = 0
        blank_lines_end = 0

        for i, line in enumerate(snippet):
            if line.strip() == "":
                blank_lines_start += 1
            else:
                break

        for i, line in enumerate(reversed(snippet)):
            if line.strip() == "":
                blank_lines_end += 1
            else:
                break

        line_range = (
            self.start_line + blank_lines_start - NUM_CONTEXT_LINES,
            self.end_line - blank_lines_end + NUM_CONTEXT_LINES,
        )

        syntax = Syntax.from_path(
            self.file,
            line_numbers=True,
            line_range=line_range,
            highlight_lines=set(range(self.start_line, self.end_line + 1)),
            padding=1,
            indent_guides=True,
        )

        syntax.stylize_range(
            Style(color="red", bold=True, underline=True),
            (self.start_line, self.start_col - 1),
            (self.end_line, self.end_col),
        )
        return syntax

    def _parse_bracketed_annotations(self, text: str) -> dict[str, typing.Any] | tuple[str]:
        """Parse annotations like "List[Stream[_T]]" into a list of strings"""
        if "[" not in text:
            return (text,)

        def _recursive_default_dict():
            return defaultdict(_recursive_default_dict)

        anns = _recursive_default_dict()
        matches = re.match(r"(?P<expression>(?P<indexer>.*?)\[(?P<indexed>.*)\])", text)

        con.log(matches.groupdict())
        for indexer, indexed in matches.groupdict().items():
            parts = [p.strip() for p in indexed.split(", ")]
            anns[indexer] = tuple((self._parse_bracketed_annotations(p) for p in parts))
        return {
            matches.group("indexer"): tuple(
                self._parse_bracketed_annotations(p.strip())
                for p in matches.group("indexed").split(",")
            )
        }

        # console.log(f"Matches: {matches.groups()}")

    def _incompatible_types_mesage(self, console: Console, options: ConsoleOptions):
        # Extract expected and actual types from message, ex.:
        #  'Argument 1 to "aiter" has incompatible type "A"; expected "B"'
        #  'Incompatible return value type (got "A", expected "B")'
        if m := re.search(
            r'has incompatible type "(.*)"; expected "(.*)"',
            self.message,
        ):
            actual, expected = m.groups()
        elif m := re.match(
            r'Incompatible return value type \(got "(.*)", expected "(.*)"\)',
            self.message,
        ):
            actual, expected = m.groups()
        elif m := re.match(
            "Incompatible types in assignment \("
            'expression has type ".*", variable has type "(.*)"\)',
            self.message,
        ):
            actual, expected = m.groups()
        elif "No return value expected" in self.message:
            actual, expected = "(value)", "NoReturn"
        else:
            raise RuntimeError(f"Unable to parse message: {self.message}")

        if "[" not in actual and "[" not in expected:
            return (
                f"Expected [bold green]{expected}[/bold green], "
                f"but got [bold red]{actual}[/bold red]"
            )

        expected_tree = AnnotationTreeNode.from_string_annotation(expected)
        actual_tree = AnnotationTreeNode.from_string_annotation(actual)

        table = Table.grid(
            Column("Expected"),
            Column("Actual"),
            expand=True,
        )

        measure_expected, measure_actual = (
            Measurement.get(
                console=console,
                options=console.options,
                renderable=tree,
            )
            for tree in (expected_tree, actual_tree)
        )

        # Calculate padding to make both columns the same width
        available = (options.max_width - 4) // 2

        lpad_expected = available // 2 - measure_expected.maximum // 2
        rpad_expected = available - measure_expected.maximum - lpad_expected

        lpad_actual = available // 2 - measure_actual.maximum // 2
        rpad_actual = available - measure_actual.maximum - lpad_actual

        padded_expected = Padding(
            expected_tree,
            (0, lpad_expected, 0, rpad_expected),
            # style=Style(bgcolor="green"),  # debugging
        )
        padded_actual = Padding(
            actual_tree,
            (0, lpad_actual, 0, rpad_actual),
            # style=Style(bgcolor="yellow"),  # debugging
        )

        table.add_row(
            Align(f"Expected [b]{expected_tree.full_expression}[/]", "center"),
            Align(f"Got [b]{actual_tree.full_expression}[/]", "center"),
        )
        table.add_row()
        table.add_row(padded_expected, padded_actual)
        return table

    def _formatted_message(self, console: Console, options: ConsoleOptions):
        if self.error_type in (MypyErrorType.ReturnValue, MypyErrorType.Argument):
            return self._incompatible_types_mesage(console, options)

        message = self.message
        if self.error_type:
            message = self.message.removesuffix(f"[{self.error_type}]")
        return message

    @logger.catch
    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        snippet = Padding(self._get_snippet(), 1)

        # subtitle =
        title = f"[yellow]{self.file}[/]"
        message = self._formatted_message(console, options)
        panel = Panel(
            Group(
                snippet,
                message,
            ),
            title=title,
            border_style="red" if self.line_type == MypyLineType.ERROR else "blue",
            subtitle=f"[red b]{self.error_type}" if self.error_type else None,
            subtitle_align="left",
            box=box.HORIZONTALS,
        )
        yield panel


PROJ_ROOT = "/home/pedro/projs/astream"


@logger.catch
async def do_check(file: Path) -> list[MypyOutputLine]:
    logger.info(f"Checking {file}")
    cmd = f"poetry run dmypy check {file}"
    logger.info(f"Running {cmd}")
    rel_file = file.relative_to(PROJ_ROOT)
    cmd = ("dmypy", "check", str(rel_file))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd="/home/pedro/projs/astream",
    )

    stdout, stderr = await proc.communicate()
    if stdout:
        logger.info(stdout.decode())
    if stderr:
        logger.error(stderr.decode())

    panel_items = [
        Text.from_markup(f"Command [blue]{shlex.join(cmd)}[/] finished with code {proc.returncode}")
    ]
    if stdout.decode().strip():

        panel_items.append(
            Panel(
                Syntax(stdout.decode(), "bash"),
                title="stdout",
                border_style="green",
                title_align="left",
            )
        )
    if stderr.decode().strip():
        panel_items.append(
            Panel(
                Group(),
                title="dmypy run output",
                border_style="blue",
                title_align="left",
            )
        )

    con.print(Padding(Group(*panel_items), (1, 2, 1, 2)))

    con.log(f"Exit code: {proc.returncode}")

    return [
        parsed
        for line in stdout.decode().splitlines()
        if (parsed := MypyOutputLine.from_line(line)) is not None
    ]


async def run_mypy_daemon() -> None:
    logger.info("Starting mypy daemon")

    kill_proc = await asyncio.create_subprocess_exec(
        "dmypy",
        "status",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd="/home/pedro/projs/astream",
    )
    await kill_proc.wait()
    logger.info("Killed mypy daemon")
    proc = await asyncio.create_subprocess_exec(
        "dmypy",
        "start",
        "--",
        "--follow-imports=skip",
        cwd="/home/pedro/projs/astream",
    )
    atexit.register(proc.kill)


@logger.catch
async def main() -> None:

    await run_mypy_daemon()

    with Live(console=con) as live:
        logger.info("Starting")

        outs = await do_check(Path("/home/pedro/projs/astream/astream/stream_utils.py"))
        for out in outs:
            if out is None:
                continue
            con.print(Padding(out, (1, 2, 0, 2)))

        async for changes in awatch(
            "/home/pedro/projs/astream",
            watch_filter=PythonFilter(),
        ):
            for change, path in changes:
                outs = await do_check(Path(path))
                for out in outs:
                    if out is None:
                        continue
                    con.print(Padding(out, (2, 2)))
                # console.print(outs)
                # output = Group(*outs)
                # live.update(Panel(outs[0]), refresh=True)


"""
import json
import inspect
import astream
import rich

with open(".mypy_cache/3.10/astream/utils.data.json") as fd:
    mypy_names = json.load(fd)

for name, obj in inspect.getmembers(astream.utils):
    if (r := mypy_names["names"].get(name)):
        rich.print(name, r)
        rich.print("========")
        
# Actually just need `module_public` to be `True` or non-existent -

   ...: import json
   ...: import inspect
   ...: import astream
   ...: import rich
   ...:
   ...: with open(".mypy_cache/3.10/astream/utils.data.json") as fd:
   ...:     mypy_names = json.load(fd)
   ...:
   ...: for name, obj in inspect.getmembers(astream.utils):
   ...:     if (r := mypy_names["names"].get(name)) and r.get("module_public", True):
   ...:         rich.print(name, r)
   ...:         rich.print("========")
   
dmypy suggest astream/formal.py:611
"""

asyncio.run(main())
