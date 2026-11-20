"""Unit tests for ``ResponseQueues``: the queue each batch reads and the late queues that wait for
the answers of batches that gave up. Uses the fake queue of ``fake_dragon``; the real Dragon queue
is checked by ``tests/integration/test-hpc/dragon/test_ai_response_queues.py``.

Not covered offline: Dragon's own behaviour on ``close()`` and on a ``put()`` after the owner closed
the queue (the fake queue cannot model it): that is a cluster check.
"""

import asyncio
import logging
import queue as stdlib_queue
import time

import pytest

pytest.importorskip(
    "dragon.ai.inference",
    reason="rhapsody.backends.ai needs dragon.ai.inference (Dragon HPC runtime)",
)

from fake_dragon import FakeQueue

from rhapsody.backends.ai import vllm as vllm_mod
from rhapsody.backends.ai.vllm import ResponseQueues


@pytest.fixture(autouse=True)
def fast_drain(monkeypatch):
    monkeypatch.setattr(vllm_mod, "_DRAIN_INTERVAL_S", 0.005)


def make():
    made = []

    def factory():
        made.append(FakeQueue())
        return made[-1]

    return ResponseQueues(factory), made


async def until(condition, seconds=3):
    async with asyncio.timeout(seconds):
        while not condition():
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_many_batches_that_were_fully_read_use_one_queue():
    queues, made = make()
    first = await queues.queue_for_batch()
    for _ in range(5):
        queues.give_up(first, 0)  # nothing missing
        assert await queues.queue_for_batch() is first
    assert len(made) == queues.created_queue_count == 1
    assert queues.late_queue_count == 0


@pytest.mark.asyncio
async def test_a_batch_that_gave_up_never_gets_its_queue_again():
    queues, made = make()
    first = await queues.queue_for_batch()
    queues.give_up(first, 3)
    second = await queues.queue_for_batch()
    assert second is not first
    assert queues.late_queues == {first: 3}
    assert queues.created_queue_count == 2
    await queues.close(workers_gone=True)


@pytest.mark.asyncio
async def test_late_answers_are_discarded_and_the_queue_is_closed_after_the_last_one():
    queues, _ = make()
    late = await queues.queue_for_batch()
    queues.give_up(late, 3)
    late.put("x")
    await until(lambda: late.empty() and queues.late_queues == {late: 2})
    assert not late.closed
    late.put("y")
    late.put("z")
    await until(lambda: queues.late_queue_count == 0)
    assert late.closed and late.empty()


@pytest.mark.asyncio
async def test_a_queue_that_never_gets_its_answers_stays_until_close():
    queues, _ = make()
    late = await queues.queue_for_batch()
    queues.give_up(late, 2)
    await asyncio.sleep(0.05)
    assert queues.late_queues == {late: 2} and not late.closed
    await queues.close(workers_gone=True)
    assert late.closed and queues.late_queue_count == 0


@pytest.mark.asyncio
async def test_the_drain_task_ends_when_nothing_is_left_and_starts_again_later():
    queues, _ = make()
    first = await queues.queue_for_batch()
    queues.give_up(first, 1)
    first.put("x")
    await until(lambda: queues.late_queue_count == 0)
    await until(lambda: queues._drainer.done())
    second = await queues.queue_for_batch()
    queues.give_up(second, 1)
    second.put("y")
    await until(lambda: queues.late_queue_count == 0)


@pytest.mark.asyncio
async def test_extra_answers_do_not_make_the_count_negative():
    queues, _ = make()
    late = await queues.queue_for_batch()
    queues.give_up(late, 2)
    for i in range(5):
        late.put(i)
    await until(lambda: queues.late_queue_count == 0)
    assert late.closed
    assert len(late) == 3  # only the 2 missing answers are read; the extras are left in it


@pytest.mark.asyncio
async def test_one_tick_reads_at_most_the_item_budget_and_queues_take_turns(monkeypatch):
    monkeypatch.setattr(vllm_mod, "_DRAIN_MAX_ITEMS_PER_TICK", 4)
    monkeypatch.setattr(vllm_mod, "_DRAIN_INTERVAL_S", 10)  # one tick only
    queues, _ = make()
    first = await queues.queue_for_batch()
    second = FakeQueue()
    for i in range(10):
        first.put(i)
        second.put(i)
    queues.give_up(first, 10)
    queues._current = second
    queues.give_up(second, 10)  # the drain task runs its first tick at the next await
    await until(lambda: len(first) < 10)
    assert (len(first), len(second)) == (6, 10)  # the first queue used the whole budget of 4
    assert list(queues.late_queues) == [second, first]  # and goes to the back
    await queues.close(workers_gone=False)


@pytest.mark.asyncio
async def test_an_unreadable_queue_does_not_stop_the_draining_of_the_others(caplog):
    class Broken(FakeQueue):
        def get_nowait(self):
            raise OSError("cannot decode")

    queues = ResponseQueues(lambda: Broken())
    broken = await queues.queue_for_batch()
    queues.give_up(broken, 2)
    good = FakeQueue()
    queues._current = good
    queues.give_up(good, 1)
    good.put("x")
    with caplog.at_level(logging.WARNING):
        await until(lambda: queues.late_queues == {broken: 2})  # the good one is done
        await asyncio.sleep(0.05)  # several more ticks on the broken one
    assert good.closed and not broken.closed
    assert caplog.text.count("Cannot read a late response queue") == 1  # warned once
    await queues.close(workers_gone=True)
    assert broken.closed  # closed at the end although it could not be read


@pytest.mark.asyncio
async def test_a_queue_that_raises_empty_is_just_not_ready():
    class Flaky(FakeQueue):
        def __init__(self):
            super().__init__()
            self.fail_next = True

        def get_nowait(self):
            if self.fail_next:
                self.fail_next = False
                raise stdlib_queue.Empty
            return super().get_nowait()

    queues = ResponseQueues(Flaky)
    late = await queues.queue_for_batch()
    queues.give_up(late, 1)
    late.put("x")
    await until(lambda: queues.late_queue_count == 0)


@pytest.mark.asyncio
async def test_a_warning_says_when_many_queues_wait(caplog):
    queues, _ = make()
    with caplog.at_level(logging.WARNING):
        for _ in range(vllm_mod._WARN_LATE_QUEUES):
            queues.give_up(await queues.queue_for_batch(), 1)
        assert "wait for late answers" not in caplog.text
        await queues.queue_for_batch()
    assert f"{vllm_mod._WARN_LATE_QUEUES} response queues still wait" in caplog.text
    await queues.close(workers_gone=True)


@pytest.mark.asyncio
async def test_cancelling_a_batch_while_its_queue_is_made_leaves_a_usable_object():
    started = asyncio.Event()
    loop = asyncio.get_running_loop()

    def slow_factory():
        loop.call_soon_threadsafe(started.set)
        time.sleep(0.1)
        return FakeQueue()

    queues = ResponseQueues(slow_factory)
    task = asyncio.create_task(queues.queue_for_batch())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert queues._current is None
    assert await queues.queue_for_batch() is not None  # the next batch makes its own


@pytest.mark.asyncio
async def test_close_with_the_workers_gone_discards_the_rest_and_closes_every_queue():
    queues, _ = make()
    late = await queues.queue_for_batch()
    queues.give_up(late, 5)
    current = await queues.queue_for_batch()
    current.put("extra")
    late.put("x")
    await queues.close(workers_gone=True)
    assert late.closed and current.closed
    assert late.empty() and current.empty()
    assert queues.late_queue_count == 0
    assert await queues.queue_for_batch() is not current  # usable again, with a new queue


@pytest.mark.asyncio
async def test_close_with_the_workers_not_gone_stops_draining_and_closes_nothing(caplog):
    queues, _ = make()
    late = await queues.queue_for_batch()
    queues.give_up(late, 5)
    current = await queues.queue_for_batch()
    with caplog.at_level(logging.WARNING):
        await queues.close(workers_gone=False)
    assert not late.closed and not current.closed
    assert queues._drainer is None
    assert "Leaving 1 late response queues open" in caplog.text
    late.put("x")
    await asyncio.sleep(0.05)
    assert len(late) == 1  # nobody reads it any more


@pytest.mark.asyncio
async def test_close_without_anything_to_do_is_fine():
    queues, made = make()
    await queues.close(workers_gone=True)
    await queues.close(workers_gone=False)
    assert made == []


@pytest.mark.asyncio
async def test_close_closes_the_queues_even_if_the_drain_task_died():
    queues, _ = make()
    late = await queues.queue_for_batch()
    queues.give_up(late, 5)

    async def dies():
        raise RuntimeError("drain died")

    queues._drainer.cancel()
    queues._drainer = asyncio.create_task(dies())
    await asyncio.sleep(0)
    await queues.close(workers_gone=True)
    assert late.closed
