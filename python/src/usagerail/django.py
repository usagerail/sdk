"""Django middleware. In settings.py:

    MIDDLEWARE = [..., "usagerail.django.UsageRailMiddleware", ...]
    USAGERAIL = {"app_key": os.environ["USAGERAIL_APP_KEY"], "auth": "api-key"}

Metering custom units in a view: request.usagerail.track(tokens)
On an ASGI server like uvicorn, wrap the application with asgi() in asgi.py (see asgi's docstring), or the last batch of usage is lost at shutdown.
With your own auth: "auth": "custom", "consumer": a function, or its dotted path ("myapp.usage.consumer"), that takes the request and returns who it is.
To use request.user, put this middleware after AuthenticationMiddleware.
The remaining keys in USAGERAIL are passed to Engine as is (on_reject takes (rejection, request) and returns an HttpResponse).
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Awaitable, MutableMapping
from typing import Any, Callable

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.urls import Resolver404, resolve
from django.utils.module_loading import import_string

from . import _adapters
from ._futures import Pending
from .engine import Engine, Rejection, read_api_key, rejection_body

# One engine per process: Django creates a middleware instance every time it loads the middleware chain (often more than once in tests), so the engine is shared
_engines: dict[str, Engine[HttpRequest]] = {}
_engines_lock = threading.Lock()
# Engines the middleware actually uses (including ones passed directly in USAGERAIL), each closed by asgi() at shutdown
_middleware_engines: weakref.WeakSet[Engine[Any]] = weakref.WeakSet()

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class UsageRailMiddleware:
    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]) -> None:
        self.get_response = get_response
        config = dict(getattr(settings, "USAGERAIL", {}))
        on_reject = config.pop("on_reject", None)
        self.on_reject: Callable[[Rejection, HttpRequest], HttpResponse] | None = import_string(on_reject) if isinstance(on_reject, str) else on_reject
        self.engine: Engine[Any] = config.pop("engine", None) or _shared_engine(config)
        _middleware_engines.add(self.engine)

    def __call__(self, request: HttpRequest) -> HttpResponse:
        route = _template(request)
        decision = self.engine.begin(
            method=request.method or "GET",
            path=request.path_info,
            # Resolve through Django's URL configuration first: the route template is known from the start, and re_path regex templates match the seller's configuration exactly
            route=route,
            api_key=read_api_key(request.headers.get("Authorization"), request.headers.get("X-Api-Key")),
            context=request,
        )
        if isinstance(decision, Pending):
            decision = decision.result()
        if not decision.allowed:
            return self.on_reject(decision, request) if self.on_reject else _rejection_response(decision)
        handle = decision.handle
        request.usagerail = handle
        try:
            response = self.get_response(request)
        except BaseException:
            # Rarely reached: Django turns view exceptions into 500 responses itself. Only exceptions from other middleware get here
            self.engine.end(handle, status=500, route=route)
            raise
        engine, status = self.engine, response.status_code
        # An SSE response counts once, as soon as it starts sending: it's one long-lived connection. Units are added when it ends
        if status < 300 and response.get("Content-Type", "").lower().startswith("text/event-stream"):
            engine.stream_started(handle, route)
        # Settle once the server has sent the whole response and closed it (for a streamed response, after the last chunk). Hooked onto request.close: Django attaches that to the
        # close callbacks of the final response (BaseHandler.get_response), so it still runs when an outer middleware replaces the response (CommonMiddleware turning a 404 into a redirect that adds the slash).
        # Hooked onto this response's own close, it would never run once the response was replaced, leaving usage unrecorded and the reservation hanging.
        # A replaced response is recorded with the status code seen here
        close_request = request.close

        def close_and_end() -> None:
            try:
                close_request()
            finally:
                engine.end(handle, status=status, route=route)

        request.close = close_and_end
        return response


def _shared_engine(config: dict[str, Any]) -> Engine[HttpRequest]:
    app_key = config.pop("app_key", "")
    auth = config.pop("auth", "api-key")
    snapshot = config.pop("snapshot", True)
    consumer = config.get("consumer")
    if isinstance(consumer, str):
        config["consumer"] = import_string(consumer)
    with _engines_lock:
        engine = _engines.get(app_key)
        if engine is None:
            engine = _adapters.engine_for(app_key, auth, snapshot, config)
            _engines[app_key] = engine
        return engine


def _template(request: HttpRequest) -> str | None:
    """Django routes have no leading slash, so add one; None if nothing matched (recorded as _unmatched at the end)"""
    try:
        match = resolve(request.path_info, getattr(request, "urlconf", None))
    except Resolver404:
        return None
    return "/" + match.route if match.route is not None else None


def _rejection_response(rejection: Rejection) -> HttpResponse:
    response = JsonResponse(rejection_body(rejection), status=rejection.status)
    for name, value in rejection.headers.items():
        response[name] = value
    return response


def asgi(application: ASGIApp) -> ASGIApp:
    """Wrapper for Django on an ASGI server (uvicorn and others). In asgi.py:

        from django.core.asgi import get_asgi_application
        from usagerail.django import asgi

        application = asgi(get_asgi_application())

    Django doesn't handle lifespan itself, and uvicorn re-raises SIGTERM after finishing up, so the process ends without running atexit and, unwrapped, the last batch of usage is lost.
    Wrapped, this answers lifespan: at shutdown it reports the remaining usage first (waiting up to 5 seconds), then tells the server it's done, like the FastAPI middleware.
    Not needed with gunicorn and WSGI: it exits normally on SIGTERM, and atexit reports"""

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "lifespan":
            await application(scope, receive, send)
            return
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                # Close all engines at once and wait together, still 5 seconds at most in total. The server must stop even if reporting fails, so errors are ignored
                closing = [asyncio.wrap_future(engine.close()) for engine in list(_middleware_engines)]
                await asyncio.gather(*closing, return_exceptions=True)
                await send({"type": "lifespan.shutdown.complete"})
                return

    return app
