"""Higher-order functions, generally curried for composition.

The `fp` (Functional Programming) module is along the lines of the standard
`functools` module, providing higher-order functions for easily constructing
new functions from other functions, for writing more declarative code.
Since the functions in this module are curried, there's no need to use
`functools.partial` for partially binding arguments.
"""
import builtins
from functools import wraps
from typing import Callable, Concatenate, Iterable, ParamSpec, TypeVar, cast

from returns.curry import partial
from returns.maybe import Maybe, Nothing, Some

_A = TypeVar("_A")
_B = TypeVar("_B")
_P = ParamSpec("_P")


def always(a: _A) -> Callable[..., _A]:
    """Return the kestrel combinator ("constant" function).

    Return a callable that accepts exactly one argument of any type, but always
    returns the value `a`.

    >>> always(42)(0)
    42
    >>> always(42)('foo')
    42
    """
    return lambda _: a


def filter(predicate: Callable[[_A], bool]) -> Callable[[Iterable[_A]], Iterable[_A]]:
    """Return a callable that accepts an iterable and returns an iterator that
    yields only the items of the iterable for which `predicate(item)` is `True`.
    Strictly curried to allow composition without the use of `functools.partial`.

    >>> list(filter(lambda x: x % 2 == 0)([1, 2, 3, 4, 5]))
    [2, 4]
    """
    return partial(builtins.filter, predicate)


def find(predicate: Callable[[_A], bool]) -> Callable[[Iterable[_A]], Maybe[_A]]:
    """Return a callable that accepts an iterable and returns the first item of the
    iterable (in a `Some`) for which `predicate` returns `True`; otherwise `Nothing`.

    >>> find(bool)([])
    <Nothing>
    >>> find(lambda x: x > 42)([19, 2, 42, 55, 45])
    <Some: 55>
    >>> find(lambda x: x > 99)([19, 2, 42, 55, 45])
    <Nothing>
    """

    def go(xs: Iterable[_A]) -> Maybe[_A]:
        for x in xs:
            if predicate(x):
                return Some(x)
        return Nothing

    return go


def for_each(f: Callable[[_A], _B]) -> Callable[[Iterable[_A]], None]:
    def go(xs: Iterable[_A]) -> None:
        for x in xs:
            f(x)

    return go


def map(f: Callable[[_A], _B]) -> Callable[[Iterable[_A]], Iterable[_B]]:
    """Return a callable that accepts an iterable and returns an iterator that
    yields `f(item)` for each item in the iterable.

    >>> list(map(lambda x: 2 * x)([1, 2, 3]))
    [2, 4, 6]
    """
    return cast(Callable[[Iterable[_A]], Iterable[_B]], partial(builtins.map, f))


def thread_first(
    f: Callable[Concatenate[_A, _P], _B]
) -> Callable[_P, Callable[[_A], _B]]:
    @wraps(f)
    def without_first(*args: _P.args, **kwargs: _P.kwargs) -> Callable[[_A], _B]:
        def supply_first(t: _A) -> _B:
            return f(t, *args, **kwargs)

        return supply_first

    return without_first


# import functools
# import inspect


# class Curry(Generic[_P]):
#     _f: Callable[_P, _B]

#     def __init__(self, f: Callable[_P, _B]):
#         functools.wraps(f)(self)
#         self._f = f  # type: ignore

#     def __repr__(self) -> str:
#         return f"curry({repr(self._f)})"

#     def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _B:
#         signature = inspect.signature(self._f)
#         bound = signature.bind_partial(*args, **kwargs)
#         bound.apply_defaults()
#         arg_names = {a for a in bound.arguments.keys()}
#         parameters = {p for p in signature.parameters.keys()}
#         if parameters - arg_names == set():
#             return self._f(*args, **kwargs)
#         if isinstance(self._f, functools.partial):
#             partial = functools.partial(
#                 self._f.func, *(self._f.args + args), **self._f.keywords, **kwargs
#             )
#         else:
#             partial = functools.partial(self._f, *args, **kwargs)
#         return Curry(partial)


# def curry(f: Callable[Concatenate[_A, _P], _B]) -> Callable:
#     """
#     Get a version of ``f`` that can be partially applied

#     Example:
#         >>> f = lambda a, b: a + b
#         >>> f_curried = curry(f)
#         >>> f_curried(1)
#         functools.partial(<function <lambda> at 0x1051f0950>, a=1)
#         >>> f_curried(1)(1)
#         2

#     Args:
#         f: The function to curry
#     Returns:
#         Curried version of ``f``
#     """

#     @wraps(f)
#     def decorator(*args: object, **kwargs: object) -> Any:
#         return Curry(f)(*args, **kwargs)

#     return decorator


# def flip(f: Callable) -> Callable:
#     """
#     Reverse the order of positional arguments of `f`

#     Example:
#         >>> f = lambda a, b, c: (a, b, c)
#         >>> flip(f)('a', 'b', 'c')
#         ('c', 'b', 'a')

#     Args:
#         f: Function to flip positional arguments of
#     Returns:
#         Function with positional arguments flipped
#     """
#     return curry(lambda *args, **kwargs: f(*reversed(args), **kwargs))
