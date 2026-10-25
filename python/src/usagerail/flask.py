"""Flask extension:

    UsageRail(app, app_key=os.environ["USAGERAIL_APP_KEY"], auth="api-key")

Metering custom units in a view: g.usagerail.track(tokens)
With your own auth: auth="custom", consumer=lambda request: ...; consumer() is called in before_request,
so register your own authentication before UsageRail (before_request functions run in registration order), or read it yourself in consumer().
"""

from __future__ import annotations

from typing import Any, Callable, Literal

from flask import Flask, Request, Response, g, jsonify, request

from . import _adapters
from ._futures import Pending
from .engine import Engine, Rejection, read_api_key, rejection_body

OnReject = Callable[[Rejection, Request], Response]

# after_request puts the request to settle into the WSGI environ; _EndOnClose settles it by the final status start_response received, when the response body is closed
_END = "usagerail.end"


class UsageRail:
    def __init__(
        self,
        app: Flask | None = None,
        *,
        app_key: str = "",
        auth: Literal["api-key", "custom"] = "api-key",
        on_reject: OnReject | None = None,
        snapshot: bool = True,
        engine: Engine[Request] | None = None,
        **engine_options: Any,
    ) -> None:
        """on_reject: your own response for rejected requests. Without it, the response gets the matching status code and headers such as Retry-After, with body {error, retry_after?}.
        snapshot: save the key snapshot to the local temp directory and read it first on restart. On by default. For other options see Engine"""
        self.on_reject = on_reject
        self.engine: Engine[Request] = engine or _adapters.engine_for(app_key, auth, snapshot, engine_options)
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        app.extensions["usagerail"] = self
        app.before_request(self._before)
        app.after_request(self._after)
        app.teardown_request(self._teardown)
        # Settlement hooks into the closing of the WSGI response body, not response.call_on_close: responses that pass a file straight through, like send_file()
        # (direct_passthrough), never go through Response.close(), so anything hooked there would never run: a finished download wouldn't be recorded and the reservation would hang.
        # The cost is that these responses can't use the server's sendfile
        app.wsgi_app = _EndOnClose(app.wsgi_app)  # type: ignore[method-assign]

    def _before(self) -> Response | None:
        decision = self.engine.begin(
            method=request.method,
            path=request.path,
            # Flask has routed before before_request: weight and reservation follow the rule it matched; None if nothing matched (404, 405, redirects)
            route=_template(),
            api_key=read_api_key(request.headers.get("Authorization"), request.headers.get("X-Api-Key")),
            context=request._get_current_object(),  # type: ignore[attr-defined]
        )
        if isinstance(decision, Pending):
            decision = decision.result()
        if not decision.allowed:
            return self.on_reject(decision, request) if self.on_reject else _rejection_response(decision)
        g.usagerail = decision.handle
        return None

    def _after(self, response: Response) -> Response:
        handle = g.get("usagerail")
        if handle is None or g.get("_usagerail_ending"):
            return response
        g._usagerail_ending = True
        # The status code and Content-Type aren't final here: after_request functions registered earlier run after this one and can still change them (a 403, a different response).
        # _EndOnClose decides once it sees the final ones in start_response
        request.environ[_END] = (self.engine, handle, _template())
        return response

    def _teardown(self, error: BaseException | None) -> None:
        # after_request was never reached (an exception went all the way up to the WSGI server, e.g. with PROPAGATE_EXCEPTIONS on): record it as 500
        handle = g.get("usagerail")
        if handle is not None and not g.get("_usagerail_ending"):
            self.engine.end(handle, status=500, route=_template())


class _EndOnClose:
    def __init__(self, wsgi_app: Callable[..., Any]) -> None:
        self.wsgi_app = wsgi_app

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Any:
        sent: dict[str, Any] = {}

        def capture(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Any:
            sent["status"] = int(status.split(" ", 1)[0])
            sent["content_type"] = next((value for name, value in headers if name.lower() == "content-type"), "")
            return start_response(status, headers) if exc_info is None else start_response(status, headers, exc_info)

        body = self.wsgi_app(environ, capture)
        pending = environ.pop(_END, None)
        if pending is None:
            return body
        engine, handle, route = pending
        # Flask calls start_response before returning the body; if it somehow didn't, record 500
        status = sent.get("status", 500)
        # An SSE response counts once, as soon as it starts sending: it's one long-lived connection. Units are added when it ends
        if status < 300 and str(sent.get("content_type", "")).lower().startswith("text/event-stream"):
            engine.stream_started(handle, route)
        # Settle once the WSGI server has sent the whole response and closed it (for a streamed response, after the last chunk)
        return _SettleOnClose(body, lambda: engine.end(handle, status=status, route=route))


class _SettleOnClose:
    """WSGI response body: on close, close the original first, then settle. Settle even if the original close() raises, or usage goes unrecorded and the reservation hangs"""

    def __init__(self, body: Any, settle: Callable[[], None]) -> None:
        self._body = body
        self._settle = settle

    def __iter__(self) -> Any:
        return iter(self._body)

    def close(self) -> None:
        try:
            close = getattr(self._body, "close", None)
            if close is not None:
                close()
        finally:
            self._settle()


def _template() -> str | None:
    rule = request.url_rule
    return rule.rule if rule is not None else None


def _rejection_response(rejection: Rejection) -> Response:
    response = jsonify(rejection_body(rejection))
    response.status_code = rejection.status
    response.headers.update(rejection.headers)
    return response
