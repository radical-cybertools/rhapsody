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
functions, containers, cells and type hints among them) are the same objects with the same
contents, and ``sys.modules`` and cloudpickle's by-value registry have not changed size. It checks
that by walking those objects for every call, which costs microseconds, not the pickling. A
function holding something whose changes the walk cannot see (an instance of a class, a class or
module pickled by value) is pickled for every call, as before, and so is one that holds too much.

A pickle is kept only once two pickles in a row of the same function, each between two walks that
agree, come out the same. So a closure made for a single task is never kept, nor what it holds; and
a value another thread changes while a pickle is taken, and changes back, is not kept either: no
walk sees the value that pickle holds, but the next pickle differs from it (unless the same race
happens during that pickle too).

``dumps_call`` pickles the rest of a call after the function, continuing the function pickle's
memo, so that one unpickler loading both gives what one pickle of the two gave: an object the
function holds and an argument passes stays one object, and a by-value function among the
arguments shares the function's globals.
"""

from __future__ import annotations

import collections
import functools
import hashlib
import io
import pickle
import sys
import threading
import types
import weakref
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
# What typing's forms hold, as they pickle it: generic aliases and Annotated, TypeVars.
_TYPING_PARTS = (
    "__origin__",
    "__args__",
    "__metadata__",
    "__bound__",
    "__constraints__",
    "__default__",
)


class _UnseenError(Exception):
    """A function holds something whose changes a walk cannot see, or too much."""


def _walk(fn: Any, budget: int, held: list | None = None) -> tuple:
    """The identities and sizes of everything a by-value pickle of ``fn`` holds, in a fixed order.

    Two walks of the same objects give equal tuples, and while the objects of the first walk are
    alive (so their ids cannot be reused), equal tuples mean the same objects with the same
    contents. ``held``, if given, gets every object the walk saw, to keep them alive. Raises
    ``_UnseenError`` for an object whose changes it cannot see, or after ``budget`` objects.
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
        if held is not None:
            held.append(obj)
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
        elif isinstance(obj, type):
            # A built-in class cannot change (cloudpickle names some, as NoneType, its own way).
            if obj.__module__ != "builtins" and not _should_pickle_by_reference(obj):
                raise _UnseenError  # pickled by value: its namespace can change
        elif kind is types.ModuleType:
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
        elif kind is types.GenericAlias:
            visit(obj.__origin__)
            visit(obj.__args__)
        elif kind is types.UnionType:  # int | None: its arguments, no __origin__
            visit(obj.__args__)
        elif kind.__module__ == "typing":
            # Type hints, in a function's annotations: what they name can be any class.
            for name in _TYPING_PARTS:
                part = getattr(obj, name, None)
                if part is not None:
                    visit(part)
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


def _walk_or_none(fn: Any, budget: int, held: list | None = None) -> tuple | None:
    """``_walk``, or None for a function that cannot be kept: one holding what a walk cannot see, or
    one whose walk fails, which then only costs the cache, never the task."""
    try:
        return _walk(fn, budget, held)
    except Exception:  # noqa: BLE001 - _UnseenError, or an object the walk did not expect
        return None


class _Pickle:
    """A function's pickle, with what continues it and what it was taken from."""

    __slots__ = ("data", "fn", "globals_ref", "held", "memo", "modules", "registry", "walk")

    def __init__(self, fn, data, memo, globals_ref, walk, held, modules, registry):
        self.fn = fn
        # Every object of the walk, alive while the pickle is kept: a value replaced in a list,
        # dict or global could otherwise take a freed object's address and look unchanged.
        self.held = held
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
    objects. With ``confirm`` (the default) a pickle is kept once the next pickle of the same
    function comes out the same; without, the first is kept. Safe to use from several threads.
    """

    def __init__(
        self,
        maxsize: int = 128,
        max_bytes: int = 1 << 20,
        walk_budget: int = 512,
        protocol: int = pickle.HIGHEST_PROTOCOL,
        confirm: bool = True,
    ) -> None:
        self.maxsize = maxsize
        self.max_bytes = max_bytes
        self.walk_budget = walk_budget
        self.protocol = protocol
        self.confirm = confirm
        self._kept: collections.OrderedDict[int, _Pickle] = collections.OrderedDict()
        # The last pickle's digest of the functions not kept yet, by id: a weak reference, so a
        # function made for one task, and what it holds, goes with its task.
        self._last: collections.OrderedDict[int, tuple[weakref.ref, bytes]] = (
            collections.OrderedDict()
        )
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._kept)

    def clear(self) -> None:
        with self._lock:
            self._kept.clear()
            self._last.clear()

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
            last = self._last.get(key)
        if kept is not None and kept.fn is fn:
            if kept.modules == modules and kept.registry == registry:
                if _walk_or_none(fn, self.walk_budget) == kept.walk:
                    with self._lock:
                        if key in self._kept:
                            self._kept.move_to_end(key)
                    return kept
        last_digest = last[1] if last is not None and last[0]() is fn else None
        # Walk only for a pickle that may be kept: the first of a function is not, when confirming.
        may_keep = not self.confirm or last_digest is not None
        held: list = []
        walk = _walk_or_none(fn, self.walk_budget, held) if may_keep else None
        buffer = io.BytesIO()
        pickler = cloudpickle.CloudPickler(buffer, protocol=self.protocol)
        pickler.dump(fn)
        data = buffer.getvalue()
        memo = pickler.memo.copy()
        fresh = _Pickle(fn, data, memo, pickler.globals_ref, walk, held, modules, registry)
        digest = hashlib.blake2b(data, digest_size=16).digest() if self.confirm else b""
        # Kept if nothing changed while it was taken, and, confirming, it is the last one again.
        keep = (
            walk is not None
            and len(data) <= self.max_bytes
            and _walk_or_none(fn, self.walk_budget) == walk
            and (not self.confirm or digest == last_digest)
        )
        with self._lock:
            if kept is not None and self._kept.get(key) is kept:
                del self._kept[key]  # it no longer matches its function
            if keep:
                self._last.pop(key, None)
                self._kept[key] = fresh
                while len(self._kept) > self.maxsize:
                    self._kept.popitem(last=False)
            elif self.confirm:
                try:
                    self._last[key] = (weakref.ref(fn), digest)
                except TypeError:  # an object without weak references: never kept
                    self._last.pop(key, None)
                else:
                    self._last.move_to_end(key)
                    while len(self._last) > 8 * self.maxsize:
                        self._last.popitem(last=False)
        return fresh
