"""FastAPI / Starlette middleware. Works with both FastAPI and Starlette:

    app.add_middleware(UsageRailMiddleware, app_key=os.environ["USAGERAIL_APP_KEY"], auth="api-key")

Metering custom units in a handler: request.state.usagerail.track(tokens)
With your own auth: auth="custom", consumer=lambda request: .... The middleware runs before routing, before FastAPI dependencies (Depends) have run,
so consumer() has to work out who it is from the request itself (decoding the token, say), or use what an authentication middleware placed before it put in request.user.
WebSocket connections skip this middleware's checks and metering; use engine.begin() / end() yourself if you need them.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import MutableMapping
from typing import Any, Awaitable, Callable, Literal

from starlette.requests import Request
from starlette.routing import Host, Match, Mount
from starlette.responses import Response

from . import _adapters
from ._futures import Pending
from .engine import Admission, Engine, Rejection, read_api_key, rejection_body
from .meter import UNMATCHED_ROUTE

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
OnReject = Callable[[Rejection, Request], Response]


class UsageRailMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        app_key: str,
        auth: Literal["api-key", "custom"],
        on_reject: OnReject | None = None,
        snapshot: bool = True,
        engine: Engine[Request] | None = None,
        **engine_options: Any,
    ) -> None:
        """on_reject: your own response for rejected requests. Without it, the response gets the matching status code and headers such as Retry-After, with body {error, retry_after?}.
        snapshot: save the key snapshot to the local temp directory and read it first on restart. On by default. For other options see Engine"""
        self.app = app
        self.on_reject = on_reject
        self.engine: Engine[Request] = engine or _adapters.engine_for(app_key, auth, snapshot, engine_options)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self.app(scope, receive, self._closing_on_shutdown(send))
            return
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope, receive)
        decision = self.engine.begin(
            method=scope["method"],
            # Strip the server-level root path but keep mount paths (uvicorn has already decoded percent-encoding)
            path=_route_path(scope),
            api_key=read_api_key(request.headers.get("authorization"), request.headers.get("x-api-key")),
            context=request,
        )
        if isinstance(decision, Pending):
            admitted = await _admit(self.engine, decision, scope, receive)
            if admitted is None:
                return
            decision, receive = admitted
        if not decision.allowed:
            response = self.on_reject(decision, request) if self.on_reject else _rejection_response(decision)
            await response(scope, receive, send)
            return
        handle = decision.handle
        scope.setdefault("state", {})["usagerail"] = handle
        await self._run(scope, receive, send, handle)

    async def _run(self, scope: Scope, receive: Receive, send: Send, handle: Any) -> None:
        engine = self.engine
        status: int | None = None
        started = False
        client_gone = False

        async def tracked(message: Message) -> None:
            nonlocal status, started, client_gone
            if message["type"] == "http.response.start":
                try:
                    await send(message)
                except BaseException:
                    # The response headers couldn't be sent: the client is gone
                    client_gone = True
                    raise
                status, started = message["status"], True
                # An SSE response counts once, as soon as it starts sending: it's one long-lived connection. Units are added when it ends
                if status < 300 and _is_event_stream(message.get("headers", ())):
                    engine.stream_started(handle, _template(scope))
                return
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                engine.end(handle, status=status, route=_template(scope))

        try:
            await self.app(scope, receive, tracked)
        except asyncio.CancelledError:
            # Cancelled before the response headers went out (the client left) counts as aborted; after they went out, by status code
            engine.end(handle, **_finished(started, status, cancelled=True), route=_template(scope))
            raise
        except BaseException:
            engine.end(handle, **_finished(started, status, cancelled=client_gone), route=_template(scope))
            raise
        # A streamed response cut short, or an app that never responded: end() only records the first call, so an earlier normal finish is unaffected
        engine.end(handle, **_finished(started, status), route=_template(scope))

    def _closing_on_shutdown(self, send: Send) -> Send:
        """At app shutdown (lifespan shutdown), report the remaining usage before telling the server it's done; atexit tries once more when the process exits"""

        async def closing(message: Message) -> None:
            if message["type"] == "lifespan.shutdown.complete":
                await asyncio.wrap_future(self.engine.close())
            await send(message)

        return closing


# Read at most this much request body while waiting for a decision, then stop reading and just wait: everything read is held in memory, so a big upload could eat memory before passing auth.
# The rest stays on the connection (backpressure), and the app reads it itself once the decision is in
MAX_BUFFERED_BODY = 64 * 1024


def _finished(started: bool, status: int | None, cancelled: bool = False) -> dict[str, Any]:
    if started:
        return {"status": status}
    return {"outcome": "aborted"} if cancelled else {"status": 500}


async def _admit(engine: Engine[Request], pending: Pending[Admission | Rejection], scope: Scope, receive: Receive) -> tuple[Admission | Rejection, Receive] | None:
    """Wait for a decision (remote key check, first lease) while watching the client. If the client leaves (the task is cancelled, or http.disconnect arrives:
    uvicorn only sends that message on disconnect and doesn't cancel the task), skip the handler and, once the decision arrives, settle as aborted and refund the reservation (like Express and Fastify in Node).
    Request body read while waiting is held (up to MAX_BUFFERED_BODY) and handed to the app unchanged afterwards. Returns None if the client has left"""
    buffered: list[Message] = []

    async def watch() -> bool:
        """True if the client left; False once the body buffer is full, and reading stops (a disconnect after that is only noticed when the app reads)"""
        size = 0
        while True:
            message = await receive()
            buffered.append(message)
            if message["type"] == "http.disconnect":
                return True
            size += len(message.get("body", b""))
            if size >= MAX_BUFFERED_BODY:
                return False

    async def decide() -> Admission | Rejection:
        return await pending

    watcher = asyncio.ensure_future(watch())
    deciding = asyncio.ensure_future(decide())
    try:
        await asyncio.wait({watcher, deciding}, return_when=asyncio.FIRST_COMPLETED)
        # Buffer full and the client is still there: stop reading and just wait for the decision
        if watcher.done() and not deciding.done() and not watcher.cancelled() and watcher.exception() is None and watcher.result() is False:
            await asyncio.wait({deciding})
    except asyncio.CancelledError:
        watcher.cancel()
        deciding.cancel()
        _abort_when_decided(engine, pending, scope)
        raise
    watcher.cancel()
    try:
        await watcher
    except asyncio.CancelledError:
        pass
    if not deciding.done() or any(message["type"] == "http.disconnect" for message in buffered):
        deciding.cancel()
        _abort_when_decided(engine, pending, scope)
        return None

    async def replayed() -> Message:
        return buffered.pop(0) if buffered else await receive()

    return deciding.result(), replayed


def _abort_when_decided(engine: Engine[Request], pending: Pending[Any], scope: Scope) -> None:
    def settle(future: Any) -> None:
        if future.cancelled() or future.exception() is not None:
            return
        decision = future.result()
        if decision.allowed:
            engine.end(decision.handle, outcome="aborted", route=_template(scope))

    pending.future.add_done_callback(settle)


def _route_path(scope: Scope) -> str:
    """The path matched against the seller's templates at the start: only the server-level root path is stripped (app_root_path, e.g. a prefix added by a reverse proxy), mount paths are kept,
    consistent with _template() at settlement. When the middleware sits in a sub-app under a Mount, root_path already includes the mount path,
    and stripping it would miss the seller's full path at the start, reserving only 1 while settlement records the full template"""
    path: str = scope["path"]
    root_path: str = scope.get("app_root_path", scope.get("root_path", ""))
    if root_path and path.startswith(root_path) and len(path) > len(root_path) and path[len(root_path)] == "/":
        return path[len(root_path) :]
    return path


def _template(scope: Scope) -> str:
    """Route template: Starlette puts it in scope["route"] after routing, and FastAPI's include_router prefix is already part of path.
    Under a Mount, the mount path (what root_path has beyond the outermost one) goes in front. Unmatched requests are recorded as _unmatched"""
    route = scope.get("route")
    # path keeps the converter ({rest:path}), path_format drops it: without it, matching at the start would treat it as one segment, and /files/foo/bar wouldn't match /files/{rest}
    template = getattr(route, "path", None) or getattr(route, "path_format", None)
    if not isinstance(template, str):
        return UNMATCHED_ROUTE
    root_path: str = scope.get("root_path", "")
    app_root_path: str = scope.get("app_root_path", root_path)
    mount_prefix = root_path[len(app_root_path) :] if root_path.startswith(app_root_path) else ""
    if not mount_prefix:
        return template
    declared = _declared_mounts(scope, root_path)
    return (declared if declared is not None else _mount_template(mount_prefix, scope, route)) + template


def _declared_mounts(scope: Scope, root_path: str) -> str | None:
    """Template of the mount paths: replay routing from the outermost routes with Starlette's own matches(), joining the declared paths of the Mounts passed through
    (/accounts/{account_id:int}). Parameter values aren't matched against path segments: converted values don't match the original text (001 becomes 1),
    and a value equal to a static segment (an ID that happens to be "accounts") would be substituted in the wrong place, reporting routes that contain customer IDs. Returns None if the replay doesn't reach this request's mount"""
    replay: dict[str, Any] = {
        "type": scope["type"],
        "path": scope["path"],
        "root_path": scope.get("app_root_path", root_path),
        "headers": scope.get("headers", []),
        "path_params": {},
    }
    routes = getattr(scope.get("router"), "routes", None)
    template = ""
    while replay["root_path"] != root_path:
        matched = next((route for route in routes or () if isinstance(route, (Mount, Host)) and route.matches(replay)[0] == Match.FULL), None)
        if matched is None:
            return None
        replay.update(matched.matches(replay)[1])
        if isinstance(matched, Mount):
            template += matched.path
        routes = matched.routes
    return template


def _mount_template(prefix: str, scope: Scope, route: Any) -> str:
    """Fallback when replaying the routing doesn't get there (custom route classes, middleware that changed root_path): root_path holds concrete values, swapped back to
    {parameter name} by value. Parameters other than the route's own are the ones the mounts matched"""
    own = set(getattr(route, "param_convertors", None) or {})
    from_mounts = [(name, str(value)) for name, value in (scope.get("path_params") or {}).items() if name not in own]
    if not prefix or not from_mounts:
        return prefix
    segments = prefix.split("/")
    for name, value in from_mounts:
        for index, segment in enumerate(segments):
            if segment == value:
                segments[index] = "{" + name + "}"
                break
    return "/".join(segments)


def _is_event_stream(headers: Any) -> bool:
    for name, value in headers:
        if name.lower() == b"content-type":
            return bool(value.lower().startswith(b"text/event-stream"))
    return False


def _rejection_response(rejection: Rejection) -> Response:
    return Response(json.dumps(rejection_body(rejection)), status_code=rejection.status, headers=dict(rejection.headers), media_type="application/json")
