"""A few helpers for using concurrent.futures.Future like a Node Promise.

Everything in the engine that touches the network (remote verification, leases, sync, reporting) returns a Future: Flask and Django threads block on it,
FastAPI awaits it. Waiting doesn't hold a thread, so hundreds of requests waiting for leases at once don't exhaust the thread pool (the "at most 300 ms extra" rule).
Callbacks run on whichever thread completes the Future, and adding a callback to a completed Future runs it on the spot, so engine state is always changed under the lock.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, InvalidStateError
from typing import Any, Callable, Generator, Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


def resolved(value: T) -> Future[T]:
    future: Future[T] = Future()
    future.set_result(value)
    return future


def settle(future: Future[T], value: T) -> None:
    """A Future that already has a result isn't changed (when a timeout and completion race, the first wins)"""
    try:
        future.set_result(value)
    except InvalidStateError:
        pass


def fail(future: Future[Any], error: BaseException) -> None:
    try:
        future.set_exception(error)
    except InvalidStateError:
        pass


def error_of(future: Future[Any]) -> BaseException | None:
    """The exception of a completed Future; cancellation counts as an error"""
    if future.cancelled():
        return RuntimeError("cancelled")
    return future.exception()


def then(
    future: Future[T],
    on_value: Callable[[T], U | Future[U]],
    on_error: Callable[[BaseException], U | Future[U]] | None = None,
) -> Future[U]:
    """Promise.then: the callback can return a value or a Future (which gets unwrapped)"""
    out: Future[U] = Future()

    def run(callback: Callable[[Any], U | Future[U]], argument: Any) -> None:
        try:
            result = callback(argument)
        except BaseException as error:  # noqa: BLE001 - passed on as is to the next step
            fail(out, error)
            return
        if isinstance(result, Future):
            result.add_done_callback(lambda inner: _copy(inner, out))
        else:
            settle(out, result)

    def done(completed: Future[T]) -> None:
        error = error_of(completed)
        if error is None:
            run(on_value, completed.result())
        elif on_error is not None:
            run(on_error, error)
        else:
            fail(out, error)

    future.add_done_callback(done)
    return out


def _copy(source: Future[Any], target: Future[Any]) -> None:
    error = error_of(source)
    if error is None:
        settle(target, source.result())
    else:
        fail(target, error)


class Pending(Generic[T]):
    """Still waiting for the server's decision. In WSGI, .result() blocks; in ASGI, await it directly"""

    __slots__ = ("future",)

    def __init__(self, future: Future[T]) -> None:
        self.future = future

    def result(self, timeout: float | None = None) -> T:
        return self.future.result(timeout)

    def __await__(self) -> Generator[Any, None, T]:
        # shield: cancelling the waiter (the client left) must not cancel the underlying Future. The engine still delivers its result (a lease, say),
        # and if it were cancelled the result would have nowhere to go and the reservation would hang; the caller decides how to clean up after a cancel
        return asyncio.shield(asyncio.wrap_future(self.future)).__await__()
