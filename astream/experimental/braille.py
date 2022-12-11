# last braille unicode character - 0x28ff
# first braille unicode character - 0x2800
# braille unicode character range - 0x2800 - 0x28ff
# To get the braille unicode character for a given number, add 0x2800 to the number.
# The pattern for the braille unicode characters is:
# 0x2800 + 0x1 * 1 + 0x2 * 2 + 0x4 * 4 + 0x8 * 8 + 0x10 * 16 + 0x20 * 32 + 0x40 * 64 + 0x80 * 128
import itertools
import math
import operator
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import reduce, partialmethod
from typing import Sequence, Type, TypeVar, NamedTuple, Iterator, Callable, Literal, Final

BRAILLE_RANGE_START = 0x2800

tup_to_braille_offset = {
    (0, 3): 1 << 0,
    (0, 2): 1 << 1,
    (0, 1): 1 << 2,
    (0, 0): 1 << 6,
    (1, 3): 1 << 3,
    (1, 2): 1 << 4,
    (1, 1): 1 << 5,
    (1, 0): 1 << 7,
}


def coords_to_unicode(*braille: tuple[int, int]) -> str:
    """Converts one or more tuples of (x, y) values in the range (0, 0) - (1, 3) (inclusive) to a
    unicode braille character composed by the dots at those coordinates.

    Args:
        braille: A tuple of (row, column) pairs representing a four row braille character.

    Returns:
        A unicode braille character.

    Examples:
        >>> coords_to_unicode((0, 0), (0, 1), (0, 2), (0, 3))
        ... '⠁'
        >>> coords_to_unicode((1, 0), (1, 1), (1, 2), (1, 3))
        ... '⠉'
        >>> coords_to_unicode((0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (1, 2), (1, 3))
        ... '⠛'
    """
    # return chr(sum(four_row_braille_dict[braille_char] for braille_char in braille) + 0x2800)
    return chr(reduce(operator.or_, [tup_to_braille_offset[tup] for tup in braille], 0x2800))


y = (0, 1, 2, 3, 2, 2, 0, 1, 2, 3, 3, 2, 1, 0)

for l, r in zip(y[::2], y[1::2]):
    print(coords_to_unicode((0, l), (1, r)), end="")


def sequence_to_braille(sequence: Sequence[int]) -> str:
    """Converts a sequence of characters to a sequence of braille characters.

    Args:
        sequence: A sequence of characters.

    Returns:
        A sequence of braille characters.

    Examples:
        >>> sequence_to_braille("abc")
        ... '⠁⠃⠉'
        >>> sequence_to_braille("123")
        ... '⠼⠁⠃⠉'
    """
    return "".join(coords_to_unicode((0, l), (1, r)) for l, r in zip(sequence[::2], sequence[1::2]))


def bouncing_ball() -> None:
    pos_x = 0
    pos_y = 0
    speed_x = 0.5
    speed_y = 0.2

    hor_start = 5
    hor_end = 45

    while True:

        pos_x += speed_x
        pos_y += speed_y

        if pos_x > hor_end:
            pos_x = hor_end
            speed_x *= -1
        elif pos_x < hor_start:
            pos_x = hor_start
            speed_x *= -1

        if pos_y >= 3:
            pos_y = 3
            speed_y *= -1
        elif pos_y < 0:
            pos_y = 0
            speed_y *= -1

        div, mod = divmod(pos_x, 1)  # % 80
        leading_spaces = " " * (int(div) + hor_start)
        braille_x = 0 if mod < 0.5 else 1
        braille_y = pos_y // 1
        # print(mod, braille_x, braille_y)

        s = leading_spaces + coords_to_unicode((braille_x, braille_y))

        # print("\r" + " " * (hor_end + hor_start + 1), end="\r", flush=False)
        print(s, end="\r", flush=True)
        time.sleep(0.01)


@dataclass
class BouncingBall:
    pos_x: float = 0
    pos_y: float = 0
    speed_x: float = 0.5
    speed_y: float = 0.2

    hor_start: float = 5
    hor_end: float = 45

    def update(self) -> None:
        self.pos_x += self.speed_x
        self.pos_y += self.speed_y

        if self.pos_x > self.hor_end:
            self.pos_x = self.hor_end
            self.speed_x *= -1
        elif self.pos_x < self.hor_start:
            self.pos_x = self.hor_start
            self.speed_x *= -1

        if self.pos_y >= 3:
            self.pos_y = 3
            self.speed_y *= -1
        elif self.pos_y < 0:
            self.pos_y = 0
            self.speed_y *= -1

    def braille_offset_and_character(self) -> tuple[int, int]:
        div, mod = divmod(self.pos_x, 1)
        offset = int(div)
        braille_x = 0 if mod < 0.5 else 1
        braille_y = round(self.pos_y)
        braille_char = tup_to_braille_offset[(braille_x, braille_y)]
        return offset, braille_char


def juggle_balls() -> None:
    n_balls = 10
    end = 40
    start = 5
    print()
    balls = [
        BouncingBall(
            pos_x=random.randint(start, end),
            pos_y=random.randint(0, 4),
            hor_start=start,
            hor_end=end,
            speed_x=random.uniform(0.3, 2) * random.choice((1, -1)),
            speed_y=random.uniform(0.1, 0.4) * random.choice((1, -1)),
        )
        for _ in range(n_balls)
    ]
    while True:
        braille_chars = [0x2800] * (end - start)
        for ball in balls:
            ball.update()
            offset, char = ball.braille_offset_and_character()
            braille_chars[offset - start - 1] |= char
        clear_screen = " " * (start + end) + "\r"
        str_start = " " * (start - 2) + "█"
        str_field = "".join(map(chr, braille_chars))
        str_end = "█"
        # print(, end="\r", flush=False)
        print(f"{clear_screen}{str_start}{str_field}{str_end}", end="  \r", flush=True)
        time.sleep(0.03)


_CanvasT = TypeVar("_CanvasT", bound="Canvas")


class Point(NamedTuple):
    x: int
    y: int

    def bresenham(self, other: "Point") -> Iterator["Point"]:
        """Yields all points on the line between self and other."""
        x0, y0 = self
        x1, y1 = other
        m_new = 2 * (y1 - y0)
        slope_error_new = m_new - (x1 - x0)

        y = y0
        for x in range(x0, x1 + 1):
            yield Point(x, y)
            slope_error_new = slope_error_new + m_new
            if slope_error_new >= 0:
                y = y + 1
                slope_error_new = slope_error_new - 2 * (x1 - x0)


def frange(start: float, stop: float, step: float) -> Iterator[float]:
    """Yields all floats between start and stop with step size step."""
    if stop < start:
        step = -step
    while start <= stop:
        yield start
        start += step


class Canvas:

    __slots__ = ("width", "height", "_canvas")

    BRAILLE_COLS: Final[int] = 2
    BRAILLE_ROWS: Final[int] = 4

    def __init__(self, width: int, height: int, contents: int = 0) -> None:
        self.width = width
        self.height = height
        self._canvas = contents

    @classmethod
    def with_dots_size(cls: Type[_CanvasT], width: int, height: int) -> _CanvasT:
        return cls(math.ceil(width / cls.BRAILLE_COLS), math.ceil(height / cls.BRAILLE_ROWS))

    def to_char_xy(self, x: int, y: int) -> int:
        return y * self.width + x

    def to_cell_xy(self, x: int, y: int) -> tuple[int, int]:
        return x // self.BRAILLE_COLS, y // self.BRAILLE_ROWS

    def to_cell_offset(self, x: int, y: int) -> int:
        return (y % self.BRAILLE_ROWS) * self.BRAILLE_COLS + (x % self.BRAILLE_COLS)

    def set_cell(self, x: int, y: int) -> "Canvas":
        cell_x, char_x = divmod(x, self.BRAILLE_COLS)
        cell_y, char_y = divmod(y, self.BRAILLE_ROWS)
        char = tup_to_braille_offset[(char_x, char_y)]
        self._canvas |= char << self.to_char_xy(cell_x, cell_y) * 8
        return self

    def clear_cell(self, x: int, y: int) -> "Canvas":
        cell_x, char_x = divmod(x, self.BRAILLE_COLS)
        cell_y, char_y = divmod(y, self.BRAILLE_ROWS)
        char = tup_to_braille_offset[(char_x, char_y)]
        self._canvas &= ~(char << self.to_char_xy(cell_x, cell_y) * 8)
        return self

    def clear(self) -> "Canvas":
        return Canvas(self.width, self.height, 0)

    def get_str(self) -> str:
        lines = (
            "".join(
                chr(self._canvas >> 8 * i & 0xFF | BRAILLE_RANGE_START)
                for i in range(self.width * y, self.width * (y + 1))
            )
            for y in range(self.height - 1, -1, -1)
        )
        return "\n".join(lines)

    def draw_line(self, start: tuple[int, int], end: tuple[int, int]) -> "Canvas":
        reduced = self._canvas
        for x, y in Point(*start).bresenham(Point(*end)):
            cell_x, char_x = divmod(x, self.BRAILLE_COLS)
            cell_y, char_y = divmod(y, self.BRAILLE_ROWS)
            char = tup_to_braille_offset[(char_x, char_y)]
            char_xy = cell_y * self.width + cell_x
            reduced |= char << char_xy * 8
        return Canvas(self.width, self.height, reduced)

    def draw_arc(
        self,
        center: tuple[int, int],
        radius: int,
        start_angle: float = 0,
        end_angle: float = 360,
        angle_step: float = 1,
    ) -> "Canvas":
        """Draws a circle with the given center and radius.

        Pretty sure there's a better way to do this, but ¯\_(ツ)_/¯
        """
        x, y = center
        reduced = self._canvas
        for i in frange(start_angle, end_angle, angle_step):
            cx = round(x + math.cos(math.radians(i)) * radius)
            cy = round(y + math.sin(math.radians(i)) * radius)
            if (
                0 <= cx < self.width * self.BRAILLE_COLS
                and 0 <= cy < self.height * self.BRAILLE_ROWS
            ):
                cell_x, char_x = divmod(cx, self.BRAILLE_COLS)
                cell_y, char_y = divmod(cy, self.BRAILLE_ROWS)
                char = tup_to_braille_offset[(char_x, char_y)]
                char_xy = cell_y * self.width + cell_x
                reduced |= char << char_xy * 8

        return Canvas(self.width, self.height, reduced)

    def draw_circle(self, center: tuple[int, int], radius: int) -> "Canvas":
        return self.draw_arc(center, radius, 0, 360, 1)

    def draw_rectangle(
        self,
        start: tuple[int, int],
        end: tuple[int, int],
        mode: Literal["add", "sub"] = "add",
    ) -> "Canvas":
        x0, y0 = start
        x1, y1 = end
        rectangle_points = itertools.chain(
            ((x, y0) for x in range(x0, x1)),
            ((x, y1) for x in range(x0, x1)),
            ((x0, y) for y in range(y0, y1)),
            ((x1, y) for y in range(y0, y1)),
        )
        copy = self._canvas
        reduced = 0
        for x, y in rectangle_points:
            cell_x, char_x = divmod(x, self.BRAILLE_COLS)
            cell_y, char_y = divmod(y, self.BRAILLE_ROWS)
            char = tup_to_braille_offset[(char_x, char_y)]
            char_xy = cell_y * self.width + cell_x
            reduced |= char << char_xy * 8

        if mode == "add":
            copy |= reduced
        elif mode == "sub":
            copy &= ~reduced
        else:
            raise ValueError(f"Invalid mode {mode!r}")
        return Canvas(self.width, self.height, copy)

    def apply_other(self, other: "Canvas", operation: Callable[[int, int], int]) -> "Canvas":
        # assert self.width == other.width and self.height == other.height
        return Canvas(self.width, self.height, operation(self._canvas, other._canvas))

    __or__ = partialmethod(apply_other, operation=operator.or_)
    __and__ = partialmethod(apply_other, operation=operator.and_)
    __xor__ = partialmethod(apply_other, operation=operator.xor)

    def __invert__(self) -> "Canvas":
        return Canvas(self.width, self.height, ~self._canvas)

    def copy(self) -> "Canvas":
        return Canvas(self.width, self.height, self._canvas)

    def __str__(self) -> str:
        """Return the canvas as a string, joining chars and newlines to form rows."""
        return self.get_str()


c = Canvas(5, 5)
c.set_cell(0, 0)
c.set_cell(1, 0)
c.set_cell(2, 0)
c.set_cell(3, 0)
c.set_cell(4, 0)

c.set_cell(2, 1)
c.set_cell(3, 2)
c.set_cell(4, 3)
c.set_cell(5, 4)
c.set_cell(5, 6)
c.set_cell(5, 5)
c.set_cell(5, 4)
c.set_cell(5, 3)
c.set_cell(5, 2)
c.set_cell(5, 1)
c.set_cell(5, 0)

print("\n\n")
print(c.get_str())

base = 200
half = base // 2
quarter = base // 4
eighth = base // 8
one_less_eighth = base - eighth
base_minus_one = base - 1
c2 = (
    Canvas.with_dots_size(base, base)
    .draw_line((0, 0), (base_minus_one, base_minus_one))
    .draw_circle((half, half), quarter)
    .draw_rectangle((eighth, eighth), (one_less_eighth, one_less_eighth))
)

c3 = ~c2
print(c3.get_str())


print("\n\n")
print(c2.get_str())

n = base
t = 0.001
cc2 = c2.copy()


def woop():
    global n, t, c2, c3
    while True:
        c2 = cc2.copy()
        bombs = []
        bomb_layers = []

        for nb in range(0, random.randint(0, 5)):
            bombs.append(
                [random.randint(0, base), random.randint(0, base), 0, random.randint(0, 100)]
            )

        for i in range(n):
            bombs_layer = Canvas.with_dots_size(base, base)
            for bomb in bombs:
                bomb[2] += 1
                bombs_layer |= bombs_layer.draw_circle((half, half), bomb[2] * bomb[3] // 50)
            bomb_layers.append(bombs_layer)

            print(
                c2.draw_arc((eighth, eighth), i).draw_arc((one_less_eighth, one_less_eighth), i)
                | bombs_layer
            )
            time.sleep(t)

        for i in range(n):
            print(
                c2.draw_arc((eighth, one_less_eighth), i).draw_arc((one_less_eighth, eighth), i)
                # .draw_line(
                #     (eighth, int(eighth + (i / base) * (base - 2 * eighth))),
                #     (one_less_eighth, int(36 - (i / base) * (base - 2 * eighth))),
                # )
                .draw_line(
                    (eighth, int(one_less_eighth - (i / base) * (base - 2 * eighth))),
                    (one_less_eighth, int(eighth + (i / base) * (base - 2 * eighth))),
                ).draw_line(
                    (eighth, int(one_less_eighth - (i / base) * (base - 2 * eighth))),
                    (int(one_less_eighth - (i / base) * (base - 2 * eighth)), eighth),
                )
                | bomb_layers.pop()
                # .draw_line((36, ), (eighth, 36))
            )
            time.sleep(t)


# woop()


def sparkline(
    data: Sequence[float],
    width: int = 60,
    min_val: int | None = None,
    max_val: int | None = None,
) -> str:
    """Return a sparkline of the given data, with the given width."""
    if not data:
        return " " * width

    _min_val = min(data) if min_val is None else min_val
    _max_val = max(data) if max_val is None else max_val
    scale = _max_val - _min_val
    if scale == 0:
        scale = 1
    # todo - something here should be doubled due to double resolution for braille
    chars = []
    for left, right in zip(data[::2], data[1::2]):
        val_left = math.floor((left - _min_val) / scale * 3)
        val_right = math.floor((right - _min_val) / scale * 3)

        ch = tup_to_braille_offset[(0, val_left)] | tup_to_braille_offset[(1, val_right)]
        chars.append(chr(BRAILLE_RANGE_START | ch))
    if len(data) % 2:
        val = math.floor((data[-1] - _min_val) / scale * 3)
        chars.append(chr(BRAILLE_RANGE_START | tup_to_braille_offset[(0, val)]))
    if len(chars) > width:
        chars = chars[:width]
    elif len(chars) < width:
        chars = [" " * (width - len(chars))] + chars
    return "".join(chars)


d = deque(maxlen=160)
i = 0
while True:
    i += 0.2
    d.append(math.sin(i))
    s = "\r" + " " * 80 + "\r" + sparkline(d)
    print(s, end="\r", flush=True)
    time.sleep(0.01)
