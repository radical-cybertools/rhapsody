"""A task's function is pickled once while nothing its pickle holds has changed.

``FunctionPickles`` gives the same pickle of a function again while the function and everything a
by-value pickle of it holds are the same objects with the same contents: its code, closure,
defaults, attributes and the globals its code reads. A change to any of them, a new module in
``sys.modules`` or a function that holds something whose changes it cannot see gets a new pickle,
so a task always runs what the caller would have pickled at that moment.
"""

import io
import pickle
import sys
import types

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

from rhapsody.backends.execution._function_pickles import FunctionPickles  # noqa: E402


def load(data):
    return pickle.loads(data)


def load_call(fn_data, rest_data):
    """As a worker loads ``dumps_call``'s two pickles: one unpickler, two loads."""
    unpickler = pickle.Unpickler(io.BytesIO(fn_data + rest_data))
    return unpickler.load(), unpickler.load()


def by_value_module(source, **values):
    """Functions of a module that is not importable, so cloudpickle pickles them by value."""
    namespace = {"__name__": "rhapsody_test_dynamic_module", **values}
    exec(source, namespace)
    return namespace


def square(x):
    return x * x


def make_adder(n):
    def add(x):
        return x + n

    return add


def wrapped_twice(func):
    """A function inside two closures, the way a framework wraps a task for AsyncFlow."""

    def with_kwargs(kwargs):
        return func(**kwargs)

    def in_context(*args, **kwargs):
        kwargs.pop("_context", None)
        return with_kwargs(*args, **kwargs)

    return in_context


def test_the_same_closure_gives_the_same_pickle():
    pickles = FunctionPickles(confirm=False)
    fn = wrapped_twice(make_adder(3))
    first = pickles.dumps(fn)
    assert pickles.dumps(fn) is first
    assert load(first)({"x": 4}) == 7


def test_a_function_pickled_by_reference_is_kept_too():
    pickles = FunctionPickles(confirm=False)
    assert pickles.dumps(square) is pickles.dumps(square)
    assert load(pickles.dumps(square)) is square


def test_distinct_closures_get_their_own_pickles():
    pickles = FunctionPickles(confirm=False)
    adders = [make_adder(i) for i in range(5)]
    assert [load(pickles.dumps(fn))(10) for fn in adders] == [10, 11, 12, 13, 14]


def test_a_closure_whose_variable_is_rebound_is_pickled_again():
    def counter():
        count = 0

        def read():
            return count

        def bump():
            nonlocal count
            count += 1

        return read, bump

    pickles = FunctionPickles(confirm=False)
    read, bump = counter()
    assert load(pickles.dumps(read))() == 0
    bump()
    assert load(pickles.dumps(read))() == 1


def test_a_closure_over_a_list_changed_in_place_is_pickled_again():
    items = [1, 2]

    def total():
        return sum(items)

    pickles = FunctionPickles(confirm=False)
    assert load(pickles.dumps(total))() == 3
    items.append(10)
    assert load(pickles.dumps(total))() == 13
    items[0] = 100
    assert load(pickles.dumps(total))() == 112


def test_a_closure_over_a_nested_dict_changed_in_place_is_pickled_again():
    config = {"model": {"layers": [1, 2]}}

    def layers():
        return len(config["model"]["layers"])

    pickles = FunctionPickles(confirm=False)
    assert load(pickles.dumps(layers))() == 2
    config["model"]["layers"].append(3)
    assert load(pickles.dumps(layers))() == 3


def test_a_global_of_a_by_value_function_rebound_changed_or_added_is_seen():
    module = by_value_module(
        "def read():\n    return LIMIT, OPTIONS['mode'], globals().get('EXTRA')\n",
        LIMIT=1,
        OPTIONS={"mode": "a"},
    )
    read = module["read"]
    pickles = FunctionPickles(confirm=False)
    assert load(pickles.dumps(read))() == (1, "a", None)
    module["LIMIT"] = 2
    assert load(pickles.dumps(read))() == (2, "a", None)
    module["OPTIONS"]["mode"] = "b"
    assert load(pickles.dumps(read))() == (2, "b", None)


def test_values_replaced_twice_between_pickles_are_seen():
    """The walk compares identities, so the objects it saw must stay alive while their pickle is
    kept: otherwise a value replaced twice can take the first one's freed address. A float in a
    list, an int in a dict and a float global, each replaced twice between two pickles."""
    items = [1.5 * 1.0]
    options = {"lr": 3 * 1000}
    module = by_value_module("def read():\n    return LEVEL\n", LEVEL=0.5 * 1.0)

    def values():
        return items[0], options["lr"]

    pickles = FunctionPickles(confirm=False)
    for step in range(50):
        load(pickles.dumps(values))
        load(pickles.dumps(module["read"]))
        items[0] = (step + 2.5) * 1.0
        items[0] = (step + 7.5) * 1.0
        options["lr"] = (step + 1) * 1001
        options["lr"] = (step + 1) * 1003
        module["LEVEL"] = (step + 0.25) * 1.0
        module["LEVEL"] = (step + 0.75) * 1.0
        assert load(pickles.dumps(values))() == (step + 7.5, (step + 1) * 1003)
        assert load(pickles.dumps(module["read"]))() == step + 0.75


def test_a_global_that_appears_after_the_first_pickle_is_seen():
    module = by_value_module("def read():\n    return LATER\n")
    read = module["read"]
    pickles = FunctionPickles(confirm=False)
    with pytest.raises(NameError):
        load(pickles.dumps(read))()
    module["LATER"] = "here"
    assert load(pickles.dumps(read))() == "here"


def test_defaults_attributes_and_code_changes_are_seen():
    def f(x=1, *, y=2):
        return x + y + getattr(f, "extra", 0)

    pickles = FunctionPickles(confirm=False)
    assert load(pickles.dumps(f))() == 3
    f.__defaults__ = (10,)
    assert load(pickles.dumps(f))() == 12
    f.__kwdefaults__ = {"y": 20}
    assert load(pickles.dumps(f))() == 30
    f.extra = 100
    assert load(pickles.dumps(f))() == 130

    def g(x=1, *, y=2):
        return -1 if f else 0  # closes over f too, so its code fits f's closure

    f.__code__ = g.__code__
    assert load(pickles.dumps(f))() == -1


def test_a_function_holding_an_object_is_pickled_for_every_call():
    """It cannot see an object's changes, so it never reuses that function's pickle."""

    class Settings:
        def __init__(self):
            self.level = 1

    settings = Settings()

    def level():
        return settings.level

    pickles = FunctionPickles(confirm=False)
    first = pickles.dumps(level)
    assert pickles.dumps(level) is not first
    settings.level = 5
    assert load(pickles.dumps(level))() == 5


def test_a_callable_object_is_pickled_for_every_call():
    class Doubler:
        factor = 2

        def __call__(self, x):
            return x * self.factor

    pickles = FunctionPickles(confirm=False)
    doubler = Doubler()
    assert pickles.dumps(doubler) is not pickles.dumps(doubler)
    assert load(pickles.dumps(doubler))(4) == 8


def test_a_partial_of_a_kept_function_is_kept_until_its_arguments_change():
    import functools

    args = [1]
    part = functools.partial(sum, args)
    pickles = FunctionPickles(confirm=False)
    assert pickles.dumps(part) is pickles.dumps(part)
    args.append(2)
    assert load(pickles.dumps(part))() == 3


def test_a_new_module_in_sys_modules_drops_the_kept_pickle(monkeypatch):
    """Cloudpickle notes which loaded submodules a by-value function's code may reach."""
    pickles = FunctionPickles(confirm=False)
    fn = wrapped_twice(make_adder(1))
    first = pickles.dumps(fn)
    monkeypatch.setitem(sys.modules, "rhapsody_test_new_module", types.ModuleType("x"))
    assert pickles.dumps(fn) is not first


def test_a_closure_wrapping_a_function_with_type_hints_is_kept():
    """functools.wraps copies the wrapped function's annotations onto the closure, so the walk
    meets type hints: unions written with |, generic aliases, typing's own forms, strings."""
    import functools
    import typing

    def f(
        a: dict | None = None,
        b: dict[str, int] | None = None,
        c: typing.Optional[list[int]] = None,
        *,
        d: "str" = "",
    ) -> int | None:
        return 1

    def wrap(func):
        @functools.wraps(func)
        def run(*args, **kwargs):
            return func(*args, **kwargs)

        return run

    pickles = FunctionPickles(confirm=False)
    wrapped = wrap(f)
    assert pickles.dumps(wrapped) is pickles.dumps(wrapped)
    assert load(pickles.dumps(wrapped))() == 1


def test_a_type_hint_naming_a_class_pickled_by_value_is_not_kept():
    """Typing's generic forms hold their arguments; a class defined in a function is pickled by
    value, and its namespace can change."""
    import typing

    class Local:
        pass

    def f(items: typing.List[Local]) -> typing.Optional[int]:  # noqa: UP006, UP045
        return len(items)

    pickles = FunctionPickles(confirm=False)
    assert pickles.dumps(f) is not pickles.dumps(f)

    def g(items: typing.List[int]) -> typing.Optional[str]:  # noqa: UP006, UP045
        return None

    assert pickles.dumps(g) is pickles.dumps(g)


def test_type_hints_naming_a_class_pickled_by_value_in_any_form_are_not_kept():
    """Annotated's metadata and a TypeVar's bound and constraints are pickled with the function."""
    import typing

    class Local:
        factor = 1

    bound = typing.TypeVar("bound", bound=Local)
    constrained = typing.TypeVar("constrained", Local, int)

    def annotated(x: typing.Annotated[int, Local]):
        return x

    def with_bound(x: bound):
        return x

    def with_constraints(x: constrained):
        return x

    pickles = FunctionPickles(confirm=False)
    for fn in (annotated, with_bound, with_constraints):
        assert pickles.dumps(fn) is not pickles.dumps(fn)


def test_a_change_undone_while_pickling_is_not_kept(monkeypatch):
    """Another thread sets a value while a pickle is taken and sets it back after: no walk sees the
    value that pickle holds, but the next pickle differs from it, so it is not kept."""
    state = {"mode": "A"}

    def mode():
        return state["mode"]

    dump = cloudpickle.CloudPickler.dump
    race = [False]

    def dump_while_another_thread_writes(self, obj):
        if not race[0]:
            return dump(self, obj)
        race[0] = False
        state["mode"] = "B"
        try:
            return dump(self, obj)
        finally:
            state["mode"] = "A"

    pickles = FunctionPickles()
    monkeypatch.setattr(cloudpickle.CloudPickler, "dump", dump_while_another_thread_writes)
    assert load(pickles.dumps(mode))() == "A"
    race[0] = True
    assert load(pickles.dumps(mode))() == "B"  # the race, in this one task only
    for _ in range(3):
        assert load(pickles.dumps(mode))() == "A"
    assert len(pickles) == 1


def test_a_function_is_kept_once_a_second_pickle_comes_out_the_same():
    """A closure made for one task is never kept, nor what it holds."""
    pickles = FunctionPickles()
    closures = [make_adder(i) for i in range(300)]
    for fn in closures:
        pickles.dumps(fn)
    assert len(pickles) == 0
    second = pickles.dumps(closures[0])
    assert len(pickles) == 1 and pickles.dumps(closures[0]) is second
    third = make_adder([1])  # a closure whose state changes between its pickles

    def changing():
        return third

    for step in range(3):
        third.__defaults__ = (step,)
        assert load(pickles.dumps(changing))().__defaults__ == (step,)
    assert len(pickles) == 1


def test_a_walk_that_fails_falls_back_to_pickling_every_call(monkeypatch):
    """Whatever the walk meets, the task is pickled: an error in the walk only costs the cache."""
    from rhapsody.backends.execution import _function_pickles

    def broken(fn, budget):
        raise AttributeError("something the walk did not expect")

    monkeypatch.setattr(_function_pickles, "_walk", broken)
    pickles = FunctionPickles(confirm=False)
    fn = make_adder(1)
    assert pickles.dumps(fn) is not pickles.dumps(fn)
    assert load(pickles.dumps(fn))(1) == 2


def test_a_recursive_closure_is_pickled_and_kept():
    def make():
        def fact(n):
            return 1 if n <= 1 else n * fact(n - 1)

        return fact

    pickles = FunctionPickles(confirm=False)
    fact = make()
    assert pickles.dumps(fact) is pickles.dumps(fact)
    assert load(pickles.dumps(fact))(5) == 120


def test_at_most_maxsize_functions_are_kept():
    pickles = FunctionPickles(maxsize=2, confirm=False)
    fns = [make_adder(i) for i in range(3)]
    kept = [pickles.dumps(fn) for fn in fns]
    assert pickles.dumps(fns[2]) is kept[2]
    assert pickles.dumps(fns[0]) is not kept[0]  # the oldest went first
    assert len(pickles) == 2


def test_a_large_pickle_is_not_kept():
    blob = b"x" * 4096

    def size():
        return len(blob)

    pickles = FunctionPickles(max_bytes=1024, confirm=False)
    assert pickles.dumps(size) is not pickles.dumps(size)
    assert load(pickles.dumps(size))() == 4096


def test_clear_drops_everything():
    pickles = FunctionPickles(confirm=False)
    fn = make_adder(2)
    first = pickles.dumps(fn)
    pickles.clear()
    assert len(pickles) == 0
    assert pickles.dumps(fn) is not first


# ----------------------------------------------------------------------------
# dumps_call: the function's pickle, then the rest of the call, as one stream would be
# ----------------------------------------------------------------------------


def test_dumps_call_loads_the_function_and_the_rest():
    pickles = FunctionPickles(confirm=False)
    fn = wrapped_twice(make_adder(5))
    data, rest = pickles.dumps_call(fn, ("token", ({"x": 1},), {}))
    loaded, (token, args, kwargs) = load_call(data, rest)
    assert token == "token" and loaded(*args, **kwargs) == 6
    assert pickles.dumps_call(fn, ("token", ({"x": 2},), {}))[0] is data


def test_dumps_call_keeps_an_object_shared_by_the_function_and_its_arguments():
    """As one pickle of both does: the argument is the very object the function reads."""
    state = {"seen": []}
    module = by_value_module("def record(s, x):\n    STATE['seen'].append(x)\n", STATE=state)
    record = module["record"]
    pickles = FunctionPickles(confirm=False)
    for _ in range(2):  # the kept pickle, and a fresh one
        data, rest = pickles.dumps_call(record, ((state, 7), state["seen"]))
        loaded, ((arg, x), seen) = load_call(data, rest)
        assert arg is loaded.__globals__["STATE"] and seen is arg["seen"]
        loaded(arg, x)
        assert seen == [7]


def test_dumps_call_keeps_a_function_in_the_arguments_on_the_same_globals():
    module = by_value_module(
        "def put(x):\n    BOX.append(x)\n\ndef get():\n    return list(BOX)\n", BOX=[]
    )
    pickles = FunctionPickles(confirm=False)
    data, rest = pickles.dumps_call(module["put"], (module["get"],))
    put, (get,) = load_call(data, rest)
    put(3)
    assert get() == [3]


def test_dumps_call_with_a_function_pickled_for_every_call():
    class Holder:
        value = 4

    holder = Holder()

    def read(extra):
        return holder.value + extra

    pickles = FunctionPickles(confirm=False)
    data, rest = pickles.dumps_call(read, ((1,),))
    loaded, ((extra,),) = load_call(data, rest)
    assert loaded(extra) == 5


# ----------------------------------------------------------------------------
# What a function the cache does not keep costs (review of the first version)
# ----------------------------------------------------------------------------


class Tool:
    """A class of this module: its instances' methods are pickled by value per call."""

    def __init__(self, factor):
        self.factor = factor

    def scale(self, x):
        return x * self.factor


def over_instance(tool):
    def scale(x):
        return tool.scale(x)

    return scale


def kinds():
    """Each kind of task function: (name, factory giving the function of the n-th task)."""
    shared = wrapped_twice(make_adder(1))
    tool = Tool(3)
    bound = tool.scale
    closure = over_instance(tool)
    return [
        ("in two closures", lambda n: shared, lambda f, n: f({"x": n}) == n + 1),
        ("bound method", lambda n: bound, lambda f, n: f(n) == 3 * n),
        ("closure over an instance", lambda n: closure, lambda f, n: f(n) == 3 * n),
        ("a new closure per task", lambda n: make_adder(n), lambda f, n: f(1) == n + 1),
        ("by reference", lambda n: square, lambda f, n: f(n) == n * n),
    ]


@pytest.mark.parametrize("kind", range(5), ids=[k[0] for k in kinds()])
def test_every_kind_of_function_loads_with_its_call_through_one_unpickler(kind):
    name, make, check = kinds()[kind]
    pickles = FunctionPickles()
    for n in range(4):  # a first pickle, a second (kept if it can be), and the kept one
        data, rest = pickles.dumps_call(make(n), ("token", (n,), {"n": n}))
        fn, (token, args, kwargs) = load_call(data, rest)
        assert token == "token" and args == (n,) and kwargs == {"n": n}
        assert check(fn, n)


def test_a_call_that_misses_the_cache_takes_one_pickler(monkeypatch):
    """A miss pickled the function, copied the memo, then made a second pickler for the
    rest of the call: about half again the time of one pickle, for nothing."""
    made = []
    init = cloudpickle.CloudPickler.__init__

    def counting(self, *args, **kwargs):
        made.append(self)
        init(self, *args, **kwargs)

    monkeypatch.setattr(cloudpickle.CloudPickler, "__init__", counting)
    pickles = FunctionPickles()
    for name, make, _ in kinds():
        for n in range(4):
            made.clear()
            pickles.dumps_call(make(n), ("token", (n,), {}))
            assert len(made) == 1, (name, n)


def test_a_function_the_walk_cannot_see_is_not_walked_again(monkeypatch):
    from rhapsody.backends.execution import _function_pickles

    walks = []
    walk = _function_pickles._walk

    def counting(fn, budget, held=None):
        walks.append(fn)
        return walk(fn, budget, held)

    monkeypatch.setattr(_function_pickles, "_walk", counting)
    pickles = FunctionPickles()
    closure = over_instance(Tool(2))
    for n in range(6):
        load_call(*pickles.dumps_call(closure, (n,)))
    assert len(walks) <= 1


def test_a_miss_keeps_an_object_shared_by_the_function_and_its_arguments():
    """Kept or not, the function and the rest of the call load as from one pickle."""
    tool = Tool(5)
    closure = over_instance(tool)
    pickles = FunctionPickles()
    for _ in range(3):
        data, rest = pickles.dumps_call(closure, (tool,))
        fn, (arg,) = load_call(data, rest)
        assert arg is fn.__closure__[0].cell_contents


def test_a_hint_that_names_a_class_by_string_is_not_kept():
    """A ForwardRef caches its evaluation (typing.get_type_hints), which no walk sees: the kept
    pickle would then differ from a fresh one."""
    import typing

    class Foo:
        pass

    def task(a: typing.Optional["Foo"] = None):  # noqa: UP045
        return a

    pickles = FunctionPickles(confirm=False)
    first = pickles.dumps(task)
    typing.get_type_hints(task, localns={"Foo": Foo})
    assert pickles.dumps(task) is not first


def test_the_kept_pickles_stay_under_a_total_size():
    fns = []
    for blob in [b"x" * 1000 for _ in range(3)]:

        def size(blob=blob):
            return len(blob)

        fns.append(size)
    one = len(cloudpickle.dumps(fns[0]))
    pickles = FunctionPickles(confirm=False, max_total_bytes=int(2.5 * one))
    for fn in fns:
        pickles.dumps(fn)
    assert len(pickles) == 2  # the oldest went first
    assert pickles.dumps(fns[2]) is pickles.dumps(fns[2])
