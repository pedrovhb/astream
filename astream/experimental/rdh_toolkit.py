""" Raymond Hettinger's Structural Pattern Matching Toolkit


Problems Solved
---------------

1) Replace case clause constants with named constants.

        x == PI

2) Allow variables to be used in case clauses.

        x == y

3) Have case values determined by dynamic function calls.

        x == func()

4) Support regular expression matching in case clauses.

        re.search(pattern, x)

5) Support set membership testing in case clauses.

        x in some_set

6) Match inexact floating point and complex values
   that arise from rounding error.

       isclose(x, 3.141592653589793)



MIT License
-----------

Copyright © 2022 Raymond Hettinger

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

__all__ = ["Const", "Var", "FuncCall", "RegexEqual", "InSet", "Approximately"]


import cmath
import re


class Const:
    """Namespace for holding constants.

        >>> Const.pi = 3.1415926535
        >>> Const.pi
        3.1415926535

    This is used in case clauses to replace a literal
    pattern with a named constant by using the value pattern:

        >>> match 3.1415926535:
        ...     case Const.pi:
        ...         print('Matches "pi"')
        ...
        Matches "pi"

    """

    pass


class Var:
    """Namespace for holding variables.

        >>> Var.x = 10
        >>> Var.x += 1
        >>> Var.x
        11

    This is used in case clauses to replace a literal
    pattern with a variable by using the value pattern:

        >>> match 11:
        ...     case Var.x:
        ...         print('Matches "x"')
        ...
        Matches "x"

    """

    pass


class FuncCall:
    """Descriptor to convert fc.name to func(name).

    The FuncCall class is a descriptor that passes the
    attribute name to function call.

    Here we pass the attribute names x and y to the function ord:

        >>> class A:
        ...     x = FuncCall(ord)
        ...     y = FuncCall(ord)
        ...
        >>> A.x         # Calls ord('x')
        120
        >>> A.y         # Calls ord('y')
        121

    This is used in case clauses to call arbitrary functions
    using the value pattern.

    This is needed when for impure functions where the value
    can change between successive calls (otherwise you could
    use Const or Var tools shown above).

    For example, consider a language translation function translate()
    that changes its result depending on the current language setting.
    We could create a namespace with dynamic lookups:

        >>> class Directions:
        ...     north = FuncCall(translate)
        ...     south = FuncCall(translate)
        ...     east = FuncCall(translate)
        ...     west = FuncCall(translate)

    In the match/case statement, we use the value pattern
    to trigger a new function call:

        >>> def convert(direction: str) -> tuple[int, int]:
        ...     "Convert a natural language direction to coordinates"
        ...     match direction:
        ...         case Directions.north:
        ...             return 1, 0
        ...         case Directions.south:
        ...             return -1, 0
        ...         case Directions.east:
        ...             return 0,  1
        ...         case Directions.west:
        ...             return 0, -1
        ...         case _:
        ...             raise ValueError(_('Unknown direction'))

    The tool is used like this:

        >>> set_language('es')   # Spanish
        >>> convert('sur')
        (-1, 0)

        >>> set_language('fr')   # French
        >>> convert('nord')
        (1, 0)

    The case statements match the current language setting
    and will change when the language setting changes.

    """

    def __init__(self, func):
        self.func = func

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, objtype=None):
        return self.func(self.name)


class RegexEqual(str):
    """Override str.__eq__ to match a regex pattern.

    The RegexEqual class inherits from str and overrides the
    __eq__() method to match a regular expression:

        >>> RegexEqual('hello') == 'h.*o'
        True

    This is used in the match-clause (not a case clause).
    It will match cases with a regex for a literal pattern:

        >>> match RegexEqual('the tale of two cities'):
        ...     case 's...y':
        ...         print('A sad story')
        ...     case 't..e':
        ...         print('A mixed tale')
        ...     case 's..a':
        ...         print('A long read')
        ...
        A mixed tale

    """

    def __eq__(self, pattern):
        return re.search(pattern, self)


class RegexEqualCase(str):

    instances: dict[tuple[str, int], "RegexEqualCase"] = {}

    def __instancecheck__(self, other):
        return True

    def __new__(cls, pattern, flags=0):
        if (pattern, flags) in cls.instances:
            return cls.instances[(pattern, flags)]
        else:
            instance = super().__new__(cls, pattern)
            cls.instances[(pattern, flags)] = instance
            return instance

    def __init__(self, pattern, flags=0):
        self.pattern = pattern
        self.flags = flags
        self._regex = re.compile(pattern, flags)

    def __eq__(self, other):
        return self._regex.search(other, self.flags)


class RegexMatcher:
    def __init__(self, s: str) -> None:
        self.s = s
        self.match: re.Match

    def __getattr__(self, name: str) -> bool:
        return getattr(self.match, name)

    def __getitem__(self, name: str) -> str:
        return self.match.group(name)

    def __eq__(self, pattern: str) -> bool:
        if (m := re.search(pattern, self.s)) is not None:
            self.match = m
            return True
        else:
            self.match = None
            return False


class InSet(set):
    """Override set.__eq__ to test set membership.

    The InSet class inherits from set and overrides the
    __eq__() method to test for set membership:

        >>> class Colors:
        ...     warm = InSet({'red', 'orange', 'yellow'})
        ...     cool = InSet({'green', 'blue', 'indigo', 'violet'})
        ...     mixed = InSet({'purple', 'brown'})

        >>> match 'blue':
        ...     case Colors.warm:
        ...         print('warm')
        ...     case Colors.cool:
        ...         print('cool')
        ...     case Colors.mixed:
        ...         print('mixed')
        ...
        cool

    """

    def __eq__(self, elem):
        return elem in self


class Approximately(complex):
    """Allow approximate equality tests for complex and floating point numbers.

    When a calculation has possible round-off error, wrap the result
    with Approximately and put the object in the match clause:

        >>> x = 1.1 + 2.2            # Rounds up to 3.3000000000000003
        >>> match Approximately(x):
        ...     case 3.0:
        ...         print('No')
        ...     case 3.3:
        ...         print('Yes, we want this to be a match.')
        ...     case _:
        ...         print('No')
        ...
        Yes, we want this to be a match.

    """

    def __new__(cls, x, /, **kwargs):
        result = complex.__new__(cls, x)
        result.kwargs = kwargs
        return result

    def __eq__(self, other):
        try:
            return cmath.isclose(self, other)
        except TypeError:
            return NotImplemented


if __name__ == "__main__":

    import doctest

    def set_language(lang_name: str) -> None:
        global curr_language
        curr_language = globals()[lang_name]

    def translate(word: str) -> str:
        return curr_language[word]

    de = dict(north="norden", south="süden", west="westen", east="osten")
    en = dict(north="north", south="south", west="west", east="east")
    es = dict(north="norte", south="sur", west="oeste", east="este")
    fr = dict(north="nord", south="sud", west="ouest", east="est")

    set_language("en")

    print(doctest.testmod())
