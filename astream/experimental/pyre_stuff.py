from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from asyncio import CancelledError
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Iterable

import rich
import typeguard
from loguru import logger
from pyre_check.client.command_arguments import CheckArguments
from pyre_check.client.commands.check import run as pyre_run_check
from pyre_check.client.configuration import Configuration
from pyre_check.client.configuration.configuration import PartialConfiguration
from rich.abc import RichRenderable
from rich.box import Box
from rich.color import Color
from rich.console import Console, ConsoleOptions, ConsoleRenderable, Group, RenderResult
from rich.live import Live
from rich.logging import RichHandler
from rich.markdown import Heading
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.segment import Segment
from rich.spinner import Spinner
from rich.style import Style
from rich.syntax import Syntax
from rich.text import Text
from rich.tree import Tree
from watchfiles import awatch, Change, PythonFilter

from astream.experimental.rdh_toolkit import RegexEqual, RegexEqualCase, RegexMatcher

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


def ide_focus_line_url(file: Path, line: int | None = None, column: int | None = None):
    # URL request to open file in IDE, example:
    # "http://localhost:63342/api/file?file=/home/pedro/projs/astream/pyproject.toml&line=10&column=5"

    from urllib.parse import urlencode

    url = "http://localhost:63342/api/file"
    params = {"file": file}
    if line:
        params["line"] = line
    if column:
        params["column"] = column
    url += "?" + urlencode(params)
    return url


def ide_focus_line(file: Path, line: int | None = None, column: int | None = None):
    from urllib.error import HTTPError, URLError
    # URL request to open file in IDE, example:
    # "http://localhost:63342/api/file?file=/home/pedro/projs/astream/pyproject.toml&line=10&column=5"

    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    url = "http://localhost:63342/api/file"
    params = {"file": file}
    if line:
        params["line"] = line
    if column:
        params["column"] = column
    url += "?" + urlencode(params)
    req = Request(url)
    try:
        response = urlopen(req)
    except HTTPError as e:
        logger.error(f"HTTP Error: {e.code} {e.reason}")
    except URLError as e:
        logger.error(f"URL Error: {e.reason}")
    else:
        logger.info(f"IDE focus line: {response.read()}")


ide_focus_line(Path(__file__), 112, 8)


@dataclass(frozen=True)
class _TypeTree:
    type_str: str

    PARAM_NAME_COLOR: ClassVar[Style] = Style(color=Color.from_rgb(255, 255, 0))
    PARAM_TYPE_COLOR: ClassVar[Style] = Style(color=Color.from_rgb(255, 0, 255))
    RETURNTYPE_COLOR: ClassVar[Style] = Style(color=Color.from_rgb(0, 255, 255))

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment(self.type_str)

    @classmethod
    def from_str(cls, type_str: str) -> _TypeTree:
        match RegexMatcher(type_str):
            case r"\((?P<signature>.*)\) -> (?P<return_type>.*)" as m:

                params = tuple(_TypeTree.from_str(p) for p in m["signature"].split(", "))
                print(m["signature"])
                print(m["return_type"])
                # print(params)
                return SignatureTT(type_str, params, _TypeTree.from_str(m["return_type"]))
            case r"(?P<param_name>.*?): (?P<param_type>.*)" as m:
                return ParamTT(type_str, m["param_name"], m["param_type"])

            case _:
                print("no match")
                return _TypeTree(type_str)


@dataclass(frozen=True)
class SignatureTT(_TypeTree):
    params: tuple[_TypeTree, ...]
    return_type: _TypeTree

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment("(", style=Style(bold=True))

        if self.params:
            for param in self.params[:-1]:
                yield from param.__rich_console__(console, options)
                yield Segment(", ", style=Style(bold=True))
            yield from self.params[-1].__rich_console__(console, options)
        yield Segment(") -> ", style=Style(bold=True))
        yield from self.return_type.__rich_console__(console, options)


# typeguard.function_name()


@dataclass(frozen=True)
class ParamTT(_TypeTree):
    name: str
    type_: str

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment(self.name, style=self.PARAM_NAME_COLOR)
        yield Segment(": ")
        yield Segment(self.type_, style=self.PARAM_TYPE_COLOR)


# tt = _TypeTree.from_str("(ab: int, cd: str, ef: float) -> int")
# con.print(tt)
# exit()


@dataclass(frozen=True)
class _TypeArg(_TypeTree):
    @property
    def name(self) -> str:
        return self.type_str.split(":")[0]

    @property
    def value(self) -> str:
        return self.type_str.split(":")[1]


@dataclass(frozen=True)
class _TypeSignature(_TypeTree):
    args: tuple[_TypeArg]
    return_type: _TypeTree

    @classmethod
    def from_str(cls, type_str: str) -> _TypeSignature:
        if m := re.match(r"\((?P<signature>.*)\) -> (?P<return_type>.*)", type_str):
            signature = m.group("signature")
            return_type = m.group("return_type")
            args = tuple(_TypeArg(arg) for arg in signature.split(", "))
            return cls(type_str, args, _TypeTree(return_type))

    @property
    def args(self) -> tuple[_TypeArg]:
        return tuple(_TypeArg(arg) for arg in self.type_str.split(" -> ")[0].split(", "))

    @property
    def return_type(self) -> _TypeTree:
        return _TypeTree(self.type_str.split(" -> ")[1])

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Text("(", style="white bold")
        for arg in self.args:
            yield arg
        yield Text(")", style="white bold")
        yield Text(" -> ", style="yellow bold")
        yield self.return_type


@dataclass(frozen=True)
class PyreError:
    """A single line in Pyre output.

    Example format:

        {
          "line": 337,
          "column": 38,
          "stop_line": 337,
          "stop_column": 43,
          "path": "astream/pyrest.py",
          "code": 6,
          "name": "Incompatible parameter type",
          "description": "Incompatible parameter type [6]: In call `Transformer.__init__`, for 1st
                          positional only parameter expected `A` but got `B`.",
          "long_description": "Incompatible parameter type [6]: In call `Transformer.__init__`,
                               for 1st positional only parameter expected `A` but got `B`.",
          "concise_description": "Incompatible parameter type [6]:
                                  For 1st param expected `A` but got `B`."
        }
    """

    line: int
    column: int
    stop_line: int
    stop_column: int
    path: Path
    code: int
    name: str
    description: str
    long_description: str
    concise_description: str

    @classmethod
    def from_json(cls, data: dict[str, str | int]) -> PyreError:
        return cls(
            line=data["line"],
            column=data["column"],
            stop_line=data["stop_line"],
            stop_column=data["stop_column"],
            path=Path(data["path"]),
            code=data["code"],
            name=data["name"],
            description=data["description"],
            long_description=data["long_description"],
            concise_description=data["concise_description"],
        )

    @classmethod
    def from_output(cls, output: bytes) -> Iterable[PyreError]:
        """Parse Pyre output into a list of PyreError objects."""
        data = json.loads(output)
        return (cls.from_json(d) for d in data)

    @classmethod
    async def run_pyre(cls, path: Path) -> dict[Path, list[PyreError]]:
        """Run Pyre on a single file and return a list of PyreError objects."""
        proc = await asyncio.create_subprocess_exec(
            "steam-run",
            "pyre",
            "--noninteractive",
            "--output=json",
            "check",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=path,
            env={
                **dict(os.environ),
            },
        )

        try:
            stdout, stderr = await proc.communicate()
            d = defaultdict[Path, list[PyreError]](list)
            for out in cls.from_output(stdout):
                d[out.path].append(out)
            con.print_json(stdout.decode())
            return d
        except CancelledError:
            proc.kill()
            raise

    def _type_tree(self, type_str: str) -> Text:
        type_str = type_str.replace("\n", " ")

        if m := re.match(r"\((?P<signature>.*)\) -> (?P<return_type>.*)", type_str):
            # Function, e.g. `(_T) -> bool`
            signature = m.group("signature")
            return_type = m.group("return_type")
            # tree = tree.add(f"({signature}) -> {return_type}")
            # sig_tree = _TypeSignature.from_str(type_str)
            # yield sig_tree
            yield Segment(f"(", style=Style(bold=True))
            yield from self._type_tree(signature)
            yield Segment(") -> ", style=Style(bold=True))
            yield from self._type_tree(return_type)
        elif m := re.match(r"(?:(.*?), )+", type_str):
            # Tuple, e.g. `int, str, bool`
            # yield Segment("(", style=Style(bold=True))
            # args = m.groups()
            args = type_str.split(", ")
            for arg in args[:-1]:
                yield from self._type_tree(arg)
                yield Segment(", ", style=Style(bold=True))
            yield from self._type_tree(args[-1])
        elif m := re.match(r"(?P<indexer>.*?)\[(?P<indexed>.*)\]", type_str):
            indexer = m.group("indexer")
            indexed = m.group("indexed")
            yield from self._type_tree(indexer)
            yield Segment("[", style=Style(bold=True))
            yield from self._type_tree(indexed)
            yield Segment("]", style=Style(bold=True))
        else:
            yield Segment(type_str, style=Style(color="red"))
            # yield Segment(")", style=Style(bold=True))
        # yield tree

    def _get_compare_tree(self):
        """Compare expected vs. actual types.


        Example output matched:

            Invalid decoration [56]: While applying decorator `pyrest.transformer`: For 1st param
            expected `_NonPartialTransformerFnT[Variable[_T], pyrest._P, Variable[_R]]` but got `(
            async_iterable: AsyncIterable[Variable[_T]], predicate: (_T) -> bool) -> AsyncIterator[
            Variable[_T]]`."
        """
        if self.code == 56:
            # return Text("foo")
            match = re.search(
                r"expected `(?P<expected>.*)` but got `(?P<actual>.*)`", self.concise_description
            )

            # return Text(f"{expected=} {actual=}")
        else:
            return None
        if match is not None:
            expected = match.group("expected")
            actual = match.group("actual")
            yield Segment("Expected: ", style=Style(color="green"))
            yield from self._type_tree(expected)
            yield Segment("\n")
            yield Segment("Actual: ", style=Style(color="red"))
            yield from self._type_tree(actual)
            yield Segment("\n")

    def _get_snippet(self, n_context_lines: int = 3) -> Syntax:
        file_lines = self.path.read_text().splitlines()
        start_line = max(self.line - n_context_lines, 1)
        end_line = min(self.stop_line + n_context_lines, len(file_lines))

        snippet = file_lines[start_line:end_line]
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
            self.line + blank_lines_start - n_context_lines,
            self.stop_line - blank_lines_end + n_context_lines,
        )

        syntax = Syntax.from_path(
            str(self.path),
            line_numbers=True,
            line_range=line_range,
            highlight_lines=set(range(self.line, self.stop_line + 1)),
            padding=1,
            indent_guides=True,
        )

        syntax.stylize_range(
            Style(
                color="red",
                bold=True,
                underline=True,
                link=ide_focus_line_url(self.path, self.line, self.column),
            ),
            (self.line, self.column),
            (self.stop_line, self.stop_column),
        )
        return syntax

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RichRenderable:
        line_col = "red" if self.code != -1 else "blue"
        yield Rule(
            f"[{line_col}][bold][{self.code:02}][/bold] {self.name}[/{line_col}]",
            align="left",
            style=f"bold {line_col}",
        )

        # The description also contains the code and name, which we show in the rule above.
        # To avoid duplication, we remove the first part of the description.
        description_text = self.description.split(":", maxsplit=1)[1].strip()

        yield from self._get_compare_tree() or [Text(description_text)]

        yield Padding(
            Group(
                Text(description_text, end="\n\n"),
                self._get_snippet(3),
            ),
            (1, 2, 1, 3),
        )


class ModPythonFilter(PythonFilter):
    """A filter that only returns files that have been modified and are Python files.

    If a file is written to but the contents are the same, it will not be returned.
    """

    def __init__(self):
        super().__init__()
        self._file_contents: dict[str, str] = {}

    def __call__(self, change: Change, path: str) -> bool:
        if not super().__call__(change, path):
            return False
        if change != Change.modified:
            return False
        if self._file_contents.get(path) == (contents := Path(path).read_text()):
            return False

        self._file_contents[path] = contents
        return True


class PyrePanel(ConsoleRenderable):
    def __init__(self, path: Path, ignore_list: list[Path] = None, allow_list: list[Path] = None):
        self._ignore_list = ignore_list or []
        self._allow_list = allow_list or []

        self._allow_list = [p.absolute() for p in self._allow_list]
        self._ignore_list = [p.absolute() for p in self._ignore_list]

        self._is_updating = False

        self.path = path
        self.errors: dict[Path, list[PyreError]] = {}
        self._update_task: asyncio.Task | None = None
        self._update_task_running = asyncio.Event()

    # def _render_error(self, error: PyreError) -> None:

    def _render_errors_for_file(self, path: Path) -> ConsoleRenderable:
        errs = self.errors.get(path, [])
        if not errs:
            yield Text(f"No errors in {path}", style="dim")
            return
        yield Padding(
            Panel(
                Group(
                    Padding(
                        Text.from_markup(
                            f"[bold red]{len(errs)}[/] Issues found in [blue]{path}[/]",
                            justify="center",
                        ),
                        (1, 0, 2, 0),
                    ),
                    *errs,
                ),
                title=path.name,
                title_align="left",
                border_style="yellow",
                padding=(1, 3, 1, 3),
            ),
            (1, 2, 0, 2),
        )

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:

        if self._is_updating:
            yield Spinner("aesthetic", style="bold red")
            return

        if not self.errors:
            yield "No errors"
            return

        for path in self.errors:
            if path.absolute() in self._ignore_list:
                continue
            if self._allow_list and path.absolute() not in self._allow_list:
                continue
            yield from self._render_errors_for_file(path)

    def __rich_measure__(self, console: Console, options: ConsoleOptions) -> int:
        return options.max_width

    async def _update(self) -> None:
        logger.info("Updating Pyre errors")
        self._is_updating = True
        if self._update_task and not self._update_task.done():
            logger.info("Cancelling previous update task")
            self._update_task.cancel()
        self._update_task = asyncio.create_task(PyreError.run_pyre(self.path))

        self.errors = await self._update_task
        self._is_updating = False
        logger.info("[dim]Updated Pyre errors[/]")


pyre_panel = PyrePanel(
    Path("/home/pedro/projs/astream"),
    allow_list=[Path("/home/pedro/projs/astream/astream/pyrest.py")],
)
live = Live(pyre_panel, console=con, auto_refresh=False)


@logger.catch
async def main() -> None:

    with live:
        await pyre_panel._update()
        async for changes in awatch("/home/pedro/projs/astream", watch_filter=ModPythonFilter()):
            t = asyncio.create_task(pyre_panel._update())
            t.add_done_callback(lambda _: live.refresh())


if __name__ == "__main__":

    asyncio.run(main())
