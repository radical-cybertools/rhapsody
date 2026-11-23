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
    pickles = FunctionPickles()
    fn = wrapped_twice(make_adder(3))
    first = pickles.dumps(fn)
    assert pickles.dumps(fn) is first
    assert load(first)({"x": 4}) == 7


def test_a_function_pickled_by_reference_is_kept_too():
    pickles = FunctionPickles()
    assert pickles.dumps(square) is pickles.dumps(square)
    assert load(pickles.dumps(square)) is square


def test_distinct_closures_get_their_own_pickles():
    pickles = FunctionPickles()
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

    pickles = FunctionPickles()
    read, bump = counter()
    assert load(pickles.dumps(read))() == 0
    bump()
    assert load(pickles.dumps(read))() == 1


def test_a_closure_over_a_list_changed_in_place_is_pickled_again():
    items = [1, 2]

    def total():
        return sum(items)

    pickles = FunctionPickles()
    assert load(pickles.dumps(total))() == 3
    items.append(10)
    assert load(pickles.dumps(total))() == 13
    items[0] = 100
    assert load(pickles.dumps(total))() == 112


def test_a_closure_over_a_nested_dict_changed_in_place_is_pickled_again():
    config = {"model": {"layers": [1, 2]}}

    def layers():
        return len(config["model"]["layers"])

    pickles = FunctionPickles()
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
    pickles = FunctionPickles()
    assert load(pickles.dumps(read))() == (1, "a", None)
    module["LIMIT"] = 2
    assert load(pickles.dumps(read))() == (2, "a", None)
    module["OPTIONS"]["mode"] = "b"
    assert load(pickles.dumps(read))() == (2, "b", None)


def test_a_global_that_appears_after_the_first_pickle_is_seen():
    module = by_value_module("def read():\n    return LATER\n")
    read = module["read"]
    pickles = FunctionPickles()
    with pytest.raises(NameError):
        load(pickles.dumps(read))()
    module["LATER"] = "here"
    assert load(pickles.dumps(read))() == "here"


def test_defaults_attributes_and_code_changes_are_seen():
    def f(x=1, *, y=2):
        return x + y + getattr(f, "extra", 0)

    pickles = FunctionPickles()
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

    pickles = FunctionPickles()
    first = pickles.dumps(level)
    assert pickles.dumps(level) is not first
    settings.level = 5
    assert load(pickles.dumps(level))() == 5


def test_a_callable_object_is_pickled_for_every_call():
    class Doubler:
        factor = 2

        def __call__(self, x):
            return x * self.factor

    pickles = FunctionPickles()
    doubler = Doubler()
    assert pickles.dumps(doubler) is not pickles.dumps(doubler)
    assert load(pickles.dumps(doubler))(4) == 8


def test_a_partial_of_a_kept_function_is_kept_until_its_arguments_change():
    import functools

    args = [1]
    part = functools.partial(sum, args)
    pickles = FunctionPickles()
    assert pickles.dumps(part) is pickles.dumps(part)
    args.append(2)
    assert load(pickles.dumps(part))() == 3


def test_a_new_module_in_sys_modules_drops_the_kept_pickle(monkeypatch):
    """Cloudpickle notes which loaded submodules a by-value function's code may reach."""
    pickles = FunctionPickles()
    fn = wrapped_twice(make_adder(1))
    first = pickles.dumps(fn)
    monkeypatch.setitem(sys.modules, "rhapsody_test_new_module", types.ModuleType("x"))
    assert pickles.dumps(fn) is not first


def test_a_recursive_closure_is_pickled_and_kept():
    def make():
        def fact(n):
            return 1 if n <= 1 else n * fact(n - 1)

        return fact

    pickles = FunctionPickles()
    fact = make()
    assert pickles.dumps(fact) is pickles.dumps(fact)
    assert load(pickles.dumps(fact))(5) == 120


def test_at_most_maxsize_functions_are_kept():
    pickles = FunctionPickles(maxsize=2)
    fns = [make_adder(i) for i in range(3)]
    kept = [pickles.dumps(fn) for fn in fns]
    assert pickles.dumps(fns[2]) is kept[2]
    assert pickles.dumps(fns[0]) is not kept[0]  # the oldest went first
    assert len(pickles) == 2


def test_a_large_pickle_is_not_kept():
    blob = b"x" * 4096

    def size():
        return len(blob)

    pickles = FunctionPickles(max_bytes=1024)
    assert pickles.dumps(size) is not pickles.dumps(size)
    assert load(pickles.dumps(size))() == 4096


def test_clear_drops_everything():
    pickles = FunctionPickles()
    fn = make_adder(2)
    first = pickles.dumps(fn)
    pickles.clear()
    assert len(pickles) == 0
    assert pickles.dumps(fn) is not first


# ----------------------------------------------------------------------------
# dumps_call: the function's pickle, then the rest of the call, as one stream would be
# ----------------------------------------------------------------------------


def test_dumps_call_loads_the_function_and_the_rest():
    pickles = FunctionPickles()
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
    pickles = FunctionPickles()
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
    pickles = FunctionPickles()
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

    pickles = FunctionPickles()
    data, rest = pickles.dumps_call(read, ((1,),))
    loaded, ((extra,),) = load_call(data, rest)
    assert loaded(extra) == 5
