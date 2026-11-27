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
happens during that pickle too). After a pickle that differs from the last, the function is walked
again only once two pickles in a row agree, so one that changes before every call costs about what
pickling it does. A function whose pickle is larger than ``max_bytes`` is pickled for every call
with no walk.

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
# typing's forms whose whole state, as they pickle it, is in _TYPING_PARTS: generic aliases,
# Annotated, unions, special forms, TypeVars. Any other typing object is unseen: a ForwardRef, say,
# caches its evaluation where no walk looks.
_TYPING_KINDS = frozenset(
    {
        "_GenericAlias",
        "_SpecialGenericAlias",
        "_UnionGenericAlias",
        "_LiteralGenericAlias",
        "_AnnotatedAlias",
        "_CallableGenericAlias",
        "_CallableType",
        "_TupleType",
        "_ConcatenateGenericAlias",
        "_UnpackGenericAlias",
        "_DeprecatedGenericAlias",
        "_SpecialForm",
        "_LiteralSpecialForm",
        "TypeVar",
        "ParamSpec",
        "TypeVarTuple",
        "ParamSpecArgs",
        "ParamSpecKwargs",
        "_NoDefaultType",
    }
)
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
            if kind.__name__ not in _TYPING_KINDS:
                raise _UnseenError
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

    ``maxsize`` functions are kept, the least recently used dropped first, and no more than
    ``max_total_bytes`` of pickles; a function whose pickle is larger than ``max_bytes`` is never
    kept, nor walked again, nor is one holding more than ``walk_budget`` objects. With ``confirm``
    (the default) a pickle is kept once the next pickle of the same function comes out the same,
    and after a pickle that differs from the last, once two more in a row agree; without, the first
    is kept.
    A function that cannot be kept costs about what ``cloudpickle.dumps`` does. Safe to use from
    several threads.
    """

    def __init__(
        self,
        maxsize: int = 128,
        max_bytes: int = 1 << 20,
        max_total_bytes: int = 16 << 20,
        walk_budget: int = 512,
        protocol: int = pickle.HIGHEST_PROTOCOL,
        confirm: bool = True,
    ) -> None:
        self.maxsize = maxsize
        self.max_bytes = max_bytes
        self.max_total_bytes = max_total_bytes
        self.walk_budget = walk_budget
        self.protocol = protocol
        self.confirm = confirm
        self._kept: collections.OrderedDict[int, _Pickle] = collections.OrderedDict()
        self._kept_bytes = 0
        # By id, with a weak reference, so a function made for one task, and what it holds, goes
        # with its task: the last pickle's size and hash of a function not kept yet, with whether
        # to walk the next one, and the functions never kept (the walk cannot see what they hold,
        # or the pickle is too large), never walked again.
        self._last: collections.OrderedDict[int, tuple[weakref.ref, tuple]] = (
            collections.OrderedDict()
        )
        self._unseen: collections.OrderedDict[int, weakref.ref] = collections.OrderedDict()
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._kept)

    def clear(self) -> None:
        with self._lock:
            self._kept.clear()
            self._kept_bytes = 0
            self._last.clear()
            self._unseen.clear()

    def dumps(self, fn: Any) -> bytes:
        """``cloudpickle.dumps(fn)``, the kept pickle while it would come out the same."""
        kept, _pickler, _buffer, data = self._pickle(fn)
        return data

    def dumps_call(self, fn: Any, rest: Any) -> tuple[bytes, bytes]:
        """Pickle ``fn``, then ``rest`` continuing ``fn``'s pickle, as one pickle of both would.

        Load the two with one unpickler, ``fn`` first:
        ``u = pickle.Unpickler(io.BytesIO(fn_data + rest_data)); fn = u.load(); rest = u.load()``.
        """
        kept, pickler, buffer, data = self._pickle(fn)
        if pickler is None:  # the kept pickle: a pickler that continues its memo
            buffer = io.BytesIO()
            pickler = cloudpickle.CloudPickler(buffer, protocol=self.protocol)
            pickler.memo = kept.memo
            # By-value functions among the arguments that share fn's globals share them once
            # loaded.
            pickler.globals_ref = dict(kept.globals_ref)
            pickler.dump(rest)
            return data, buffer.getvalue()
        # A fresh pickle of fn: the same pickler goes on with the rest, one stream split in two.
        # The buffer starts again for the rest: writing after the function's pickle copied it.
        buffer.seek(0)
        buffer.truncate()
        pickler.dump(rest)
        return data, buffer.getvalue()

    def _pickle(self, fn: Any) -> tuple:
        """``(kept, None, None, data)`` for the kept pickle, else ``(None, pickler, buffer, data)``
        with the pickler that took ``fn``'s pickle, ready to go on."""
        key = id(fn)
        with self._lock:
            kept = self._kept.get(key)
            last = self._last.get(key)
            unseen = self._unseen.get(key)
        if kept is not None and kept.fn is fn:
            if kept.modules == len(sys.modules) and kept.registry == _registry_size():
                if _walk_or_none(fn, self.walk_budget) == kept.walk:
                    with self._lock:
                        if key in self._kept:
                            self._kept.move_to_end(key)
                    return kept, None, None, kept.data
        buffer = io.BytesIO()
        pickler = cloudpickle.CloudPickler(buffer, protocol=self.protocol)
        if unseen is not None and unseen() is fn:  # never kept: no walk, no bookkeeping
            pickler.dump(fn)
            if kept is not None:
                self._drop(key, kept)
            return None, pickler, buffer, buffer.getvalue()
        last_mark, settled = last[1] if last is not None and last[0]() is fn else (None, False)
        # Walk only for a pickle that may be kept. Confirming, that is one that may come out as the
        # last did: not the first of a function, nor one after a change, until two agree again.
        may_keep = not self.confirm or settled
        held: list = []
        walk = _walk_or_none(fn, self.walk_budget, held) if may_keep else None
        modules, registry = len(sys.modules), _registry_size()
        pickler.dump(fn)
        data = buffer.getvalue()
        if len(data) > self.max_bytes:  # never kept: not walked, nor checked, again
            with self._lock:
                if kept is not None and self._kept.get(key) is kept:
                    self._drop_locked(key)
                self._last.pop(key, None)
                self._remember(self._unseen, key, fn, None)
            return None, pickler, buffer, data
        mark = (len(data), hash(data)) if self.confirm else None
        # Kept if, confirming, it is the last one again, and nothing changed while it was taken.
        keep = (
            walk is not None
            and (not self.confirm or mark == last_mark)
            and _walk_or_none(fn, self.walk_budget) == walk
        )
        with self._lock:
            if kept is not None and self._kept.get(key) is kept:
                self._drop_locked(key)  # it no longer matches its function
            if keep:
                self._last.pop(key, None)
                # Copied before the pickler goes on with the rest of the call.
                fresh = _Pickle(
                    fn,
                    data,
                    pickler.memo.copy(),
                    dict(pickler.globals_ref),
                    walk,
                    held,
                    modules,
                    registry,
                )
                if key in self._kept:  # another thread kept its pickle meanwhile: this replaces it
                    self._drop_locked(key)
                self._kept[key] = fresh
                self._kept_bytes += len(data)
                while self._kept and (
                    len(self._kept) > self.maxsize or self._kept_bytes > self.max_total_bytes
                ):
                    self._drop_locked(next(iter(self._kept)))
            elif may_keep and walk is None:
                self._last.pop(key, None)
                self._remember(self._unseen, key, fn, None)
            elif self.confirm:
                # The next pickle is walked if this one was the first or came out as the last did.
                self._remember(self._last, key, fn, (mark, last_mark is None or mark == last_mark))
        return None, pickler, buffer, data

    def _remember(self, table: collections.OrderedDict, key: int, fn: Any, value: Any) -> None:
        """Note ``fn`` in ``table`` by a weak reference; one without weak references is not."""
        try:
            ref = weakref.ref(fn)
        except TypeError:
            table.pop(key, None)
            return
        table[key] = ref if value is None else (ref, value)
        table.move_to_end(key)
        while len(table) > 8 * self.maxsize:
            table.popitem(last=False)

    def _drop(self, key: int, kept: _Pickle) -> None:
        with self._lock:
            if self._kept.get(key) is kept:
                self._drop_locked(key)

    def _drop_locked(self, key: int) -> None:
        dropped = self._kept.pop(key)
        self._kept_bytes -= len(dropped.data)


def _registry_size() -> int:
    return len(cloudpickle.list_registry_pickle_by_value())
