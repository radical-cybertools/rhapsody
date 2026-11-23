"""Pickle a task's function once, for as long as nothing its pickle holds changes.

Backends that send function tasks to other processes pickle each task's function with cloudpickle.
A function that cloudpickle cannot import by name (a closure, a lambda, a function of ``__main__``)
is pickled by value, and that costs far more than the task's arguments: for each such function
cloudpickle scans every loaded module (``sys.modules``) for submodules its code might use. In a
process that has loaded a few thousand modules, a task whose function sits in two closures, as a
framework built on AsyncFlow wraps it, takes about a millisecond to pickle, on the event loop.

``FunctionPickles`` keeps the pickle of each function it saw last and gives it again while the
pickle would come out the same: while the function and everything a by-value pickle of it holds
(its code, closure, defaults, attributes and the globals its code reads, and so on through the
functions, containers and cells among them) are the same objects with the same contents, and
``sys.modules`` and cloudpickle's by-value registry have not changed size. It checks that by
walking those objects for every call, which costs microseconds, not the pickling. A function
holding something whose changes the walk cannot see (an instance of a class, a class or module
pickled by value) is pickled for every call, as before, and so is one that holds too much.

``dumps_call`` pickles the rest of a call after the function, continuing the function pickle's
memo, so that one unpickler loading both gives what one pickle of the two gave: an object the
function holds and an argument passes stays one object, and a by-value function among the
arguments shares the function's globals.
"""

from __future__ import annotations

import collections
import functools
import io
import pickle
import sys
import threading
import types
from typing import Any

import cloudpickle

try:  # cloudpickle's own answers, so this follows what it pickles by value
    from cloudpickle.cloudpickle import _extract_code_globals
except ImportError:  # pragma: no cover - older or newer cloudpickle

    def _extract_code_globals(code):
        names = dict.fromkeys(code.co_names)
        for const in code.co_consts:
            if isinstance(const, types.CodeType):
                names.update(_extract_code_globals(const))
        return names


try:
    from cloudpickle.cloudpickle import _should_pickle_by_reference
except ImportError:  # pragma: no cover - then every function counts as pickled by value

    def _should_pickle_by_reference(obj, name=None):
        return False


# Objects that cannot change and hold nothing that can.
_ATOMS = frozenset(
    {
        type(None),
        bool,
        int,
        float,
        complex,
        str,
        bytes,
        range,
        type(Ellipsis),
        type(NotImplemented),
        types.CodeType,
    }
)
# The globals cloudpickle keeps for a by-value function besides those its code reads.
_BASE_GLOBALS = ("__package__", "__name__", "__path__", "__file__")
# Markers in a walk; object ids are addresses, never this small.
_ABSENT, _EMPTY, _NONE = 1, 2, 3
# Deepest nesting a walk follows; deeper counts as more than it can see.
_MAX_DEPTH = 64


class _UnseenError(Exception):
    """A function holds something whose changes a walk cannot see, or too much."""


def _walk(fn: Any, budget: int) -> tuple:
    """The identities and sizes of everything a by-value pickle of ``fn`` holds, in a fixed order.

    Two walks of the same objects give equal tuples, and while the objects of the first walk are
    alive (so their ids cannot be reused), equal tuples mean the same objects with the same
    contents. Raises ``_UnseenError`` for an object whose changes it cannot see, or after ``budget``
    objects.
    """
    out: list = []
    seen: set = set()
    left = [budget]
    depth = [0]

    def visit(obj: Any) -> None:
        oid = id(obj)
        out.append(oid)
        if oid in seen:
            return
        seen.add(oid)
        left[0] -= 1
        if left[0] < 0 or depth[0] > _MAX_DEPTH:
            raise _UnseenError
        depth[0] += 1
        try:
            visit_object(obj)
        finally:
            depth[0] -= 1

    def visit_object(obj: Any) -> None:
        kind = type(obj)
        if kind in _ATOMS:
            return
        if kind is tuple or kind is list or kind is frozenset or kind is set:
            out.append(len(obj))
            for item in obj:
                visit(item)
        elif kind is dict:
            out.append(len(obj))
            for key, value in obj.items():
                visit(key)
                visit(value)
        elif kind is types.FunctionType:
            if not _should_pickle_by_reference(obj):
                visit_function(obj)
        elif kind is types.CellType:
            try:
                contents = obj.cell_contents
            except ValueError:
                out.append(_EMPTY)
            else:
                visit(contents)
        elif kind is types.ModuleType or isinstance(obj, type):
            if not _should_pickle_by_reference(obj):
                raise _UnseenError  # pickled by value: its namespace can change
        elif kind is types.BuiltinFunctionType:
            owner = obj.__self__
            if owner is not None and type(owner) is not types.ModuleType:
                raise _UnseenError  # a method of an object
        elif kind is functools.partial:
            visit(obj.func)
            visit(obj.args)
            visit(obj.keywords)
            visit(obj.__dict__)
        elif kind is types.MethodType:
            visit(obj.__func__)
            visit(obj.__self__)
        elif kind is types.GenericAlias or kind is types.UnionType:
            visit(obj.__origin__)
            visit(obj.__args__)
        elif kind.__module__ == "typing":
            return  # type hints, in a function's annotations
        else:
            raise _UnseenError

    def visit_function(fn: types.FunctionType) -> None:
        # What cloudpickle's _function_getstate and _function_getnewargs read.
        code = fn.__code__
        visit(code)
        for value in (
            fn.__name__,
            fn.__qualname__,
            fn.__module__,
            fn.__doc__,
            fn.__defaults__,
            fn.__kwdefaults__,
            fn.__annotations__,
            fn.__dict__,
        ):
            visit(value)
        closure = fn.__closure__
        if closure is None:
            out.append(_NONE)
        else:
            visit(closure)
        namespace = fn.__globals__
        for name in (*_BASE_GLOBALS, *_extract_code_globals(code)):
            if name in namespace:
                visit(namespace[name])
            else:
                out.append(_ABSENT)

    visit(fn)
    return tuple(out)


class _Pickle:
    """A function's pickle, with what continues it and what it was taken from."""

    __slots__ = ("data", "fn", "globals_ref", "memo", "modules", "registry", "walk")

    def __init__(self, fn, data, memo, globals_ref, walk, modules, registry):
        self.fn = fn
        self.data = data
        self.memo = memo
        self.globals_ref = globals_ref
        self.walk = walk
        self.modules = modules
        self.registry = registry


class FunctionPickles:
    """Pickles of the functions seen last, each given again while it would come out the same.

    ``maxsize`` functions are kept, the least recently used dropped first; a pickle larger than
    ``max_bytes`` is never kept, nor is one of a function holding more than ``walk_budget``
    objects. Safe to use from several threads.
    """

    def __init__(
        self,
        maxsize: int = 128,
        max_bytes: int = 1 << 20,
        walk_budget: int = 512,
        protocol: int = pickle.HIGHEST_PROTOCOL,
    ) -> None:
        self.maxsize = maxsize
        self.max_bytes = max_bytes
        self.walk_budget = walk_budget
        self.protocol = protocol
        self._kept: collections.OrderedDict[int, _Pickle] = collections.OrderedDict()
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._kept)

    def clear(self) -> None:
        with self._lock:
            self._kept.clear()

    def dumps(self, fn: Any) -> bytes:
        """``cloudpickle.dumps(fn)``, the kept pickle while it would come out the same."""
        return self._pickle(fn).data

    def dumps_call(self, fn: Any, rest: Any) -> tuple[bytes, bytes]:
        """Pickle ``fn``, then ``rest`` continuing ``fn``'s pickle, as one pickle of both would.

        Load the two with one unpickler, ``fn`` first:
        ``u = pickle.Unpickler(io.BytesIO(fn_data + rest_data)); fn = u.load(); rest = u.load()``.
        """
        kept = self._pickle(fn)
        buffer = io.BytesIO()
        pickler = cloudpickle.CloudPickler(buffer, protocol=self.protocol)
        pickler.memo = kept.memo
        # By-value functions among the arguments that share fn's globals share them once loaded.
        pickler.globals_ref = dict(kept.globals_ref)
        pickler.dump(rest)
        return kept.data, buffer.getvalue()

    def _pickle(self, fn: Any) -> _Pickle:
        modules = len(sys.modules)
        registry = len(cloudpickle.list_registry_pickle_by_value())
        key = id(fn)
        with self._lock:
            kept = self._kept.get(key)
        if kept is not None and kept.fn is fn:
            if kept.modules == modules and kept.registry == registry:
                try:
                    if _walk(fn, self.walk_budget) == kept.walk:
                        with self._lock:
                            if key in self._kept:
                                self._kept.move_to_end(key)
                        return kept
                except _UnseenError:
                    pass
        try:  # before pickling: a change made meanwhile makes the next walk differ
            walk = _walk(fn, self.walk_budget)
        except _UnseenError:
            walk = None
        buffer = io.BytesIO()
        pickler = cloudpickle.CloudPickler(buffer, protocol=self.protocol)
        pickler.dump(fn)
        data = buffer.getvalue()
        fresh = _Pickle(fn, data, pickler.memo.copy(), pickler.globals_ref, walk, modules, registry)
        with self._lock:
            if walk is not None and len(data) <= self.max_bytes:
                self._kept[key] = fresh
                self._kept.move_to_end(key)
                while len(self._kept) > self.maxsize:
                    self._kept.popitem(last=False)
            elif self._kept.get(key) is kept:
                self._kept.pop(key, None)
        return fresh
