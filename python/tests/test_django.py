import sys

import pytest

pytest.importorskip("django", reason="Django 6 needs Python 3.12+")

import django  # noqa: E402
from adapter_helpers import backend, bearer, stop_backend, summary  # noqa: E402
from django.conf import settings  # noqa: E402

if not settings.configured:
    settings.configure(
        DEBUG=False,
        ALLOWED_HOSTS=["*"],
        ROOT_URLCONF=__name__,
        MIDDLEWARE=["usagerail.django.UsageRailMiddleware"],
        SECRET_KEY="test-only",
        USAGERAIL={"engine": None},
        LOGGING_CONFIG=None,
    )
    django.setup()

from django.http import JsonResponse, StreamingHttpResponse  # noqa: E402
from django.test import Client, override_settings  # noqa: E402
from django.urls import include, path, re_path  # noqa: E402

holder = {}


def user(request, pk):
    return JsonResponse({"id": pk})


def generate(request):
    request.usagerail.track(42)
    return JsonResponse({"ok": True}, status=201)


def archive(request, year):
    return JsonResponse({"year": year})


def events(request):
    handle = request.usagerail
    engine = holder["engine"]
    transport = holder["transport"]

    def stream():
        yield "data: hello\n\n"
        engine.flush()
        holder["seen"] = [(row["result_type"], row["count"], row["units"]) for row in transport.rows()]
        handle.track(7)
        yield "data: bye\n\n"

    return StreamingHttpResponse(stream(), content_type="text/event-stream")


def boom(request):
    raise RuntimeError("boom")


api = [
    path("users/<int:pk>/", user),
    path("generate/", generate),
    re_path(r"^archive/(?P<year>[0-9]{4})/$", archive),
    path("events/", events),
    path("boom/", boom),
]
urlpatterns = [path("v1/", include(api))]


def client_for(engine, transport, **config):
    holder.update(engine=engine, transport=transport)
    return override_settings(USAGERAIL={"engine": engine, **config})


def test_route_templates_track_rejections_and_unmatched_paths():
    engine, transport, key, limited = backend()
    with client_for(engine, transport):
        client = Client()
        assert client.get("/v1/users/7/?expand=1", headers=bearer(key)).json() == {"id": 7}
        assert client.post("/v1/generate/", headers={"X-Api-Key": key}).status_code == 201
        invalid = client.get("/v1/users/7/", headers=bearer("acme_test_nope"))
        assert invalid.status_code == 401
        assert invalid.json() == {"error": "invalid_api_key"}
        assert client.get("/nowhere", headers=bearer(key)).status_code == 404
        assert client.get("/v1/archive/2026/", headers=bearer(key)).json() == {"year": "2026"}
        assert client.get("/v1/users/1/", headers=bearer(limited)).status_code == 200
        too_many = client.get("/v1/users/1/", headers=bearer(limited))
        assert too_many.status_code == 429
        assert too_many.headers["Retry-After"] == "1"
        assert too_many.headers["RateLimit-Policy"]
    engine.flush()
    # Resolved through the URL configuration at the start, so even requests the SDK rejects get a template; include prefixes go in front, and re_path records its regex (Django drops the leading ^ when joining)
    assert summary(transport) == [
        ("GET", "/v1/users/<int:pk>/", "2xx", 1),
        ("POST", "/v1/generate/", "2xx", 42),
        ("GET", "/v1/users/<int:pk>/", "sdk_401", 0),
        ("GET", "_unmatched", "4xx", 0),
        ("GET", "/v1/archive/(?P<year>[0-9]{4})/$", "2xx", 1),
        ("GET", "/v1/users/<int:pk>/", "2xx", 1),
        ("GET", "/v1/users/<int:pk>/", "sdk_429", 0),
    ]


def test_an_sse_response_is_metered_once_as_soon_as_it_starts_streaming():
    engine, transport, key, _ = backend()
    with client_for(engine, transport):
        response = Client().get("/v1/events/", headers=bearer(key))
        assert b"".join(response.streaming_content) == b"data: hello\n\ndata: bye\n\n"
        response.close()
    engine.flush()
    assert holder["seen"] == [("ws_open", 1, 0)]
    assert [(row["route"], row["result_type"], row["count"], row["units"]) for row in transport.rows()] == [
        ("/v1/events/", "ws_open", 1, 0),
        ("/v1/events/", "ws_open", 0, 7),
    ]


def test_a_weighted_route_reserves_its_template_weight():
    engine, transport, key = stop_backend([{"method": "GET", "route": "/v1/users/<int:pk>/", "weight": 10, "reserve": 10}])
    with client_for(engine, transport):
        assert Client().get("/v1/users/3/", headers=bearer(key)).status_code == 429


def test_an_exception_in_a_view_is_recorded_as_5xx():
    engine, transport, key, _ = backend()
    with client_for(engine, transport):
        assert Client(raise_request_exception=False).get("/v1/boom/", headers=bearer(key)).status_code == 500
    engine.flush()
    assert summary(transport) == [("GET", "/v1/boom/", "5xx", 0)]


def test_on_reject_and_a_dotted_path_consumer():
    engine, transport, _, _ = backend(auth="custom", consumer=sys.modules[__name__].consumer_from_header)
    with client_for(engine, transport):
        client = Client()
        assert client.get("/v1/users/1/", headers={"X-User": "user-1"}).status_code == 200
        assert client.get("/v1/users/1/").status_code == 200
    engine.flush()
    assert sorted(row["consumer_kind"] for row in transport.rows()) == ["anonymous", "external"]

    engine, transport, _, _ = backend()
    with client_for(engine, transport, on_reject=f"{__name__}.custom_rejection"):
        response = Client().get("/v1/users/1/")
        assert (response.status_code, response.json()) == (401, {"message": "no: invalid_api_key"})


def consumer_from_header(request):
    return request.headers.get("X-User")


def custom_rejection(rejection, request):
    return JsonResponse({"message": f"no: {rejection.error}"}, status=rejection.status)


# ---- Found in review (round four): a response replaced by an outer middleware is still settled ----
def test_a_response_replaced_by_an_outer_middleware_is_still_settled():
    # CommonMiddleware is the outer layer: /v1/users/7 has no trailing slash, the inner layer answers 404, and it replaces that with a 301 adding the slash.
    # Settlement hooked onto the response before the replacement would never run, leaving usage unrecorded and the reservation hanging
    engine, transport, key = stop_backend([{"method": "GET", "route": "/v1/users/<int:pk>/", "weight": 5, "reserve": 5}])
    with client_for(engine, transport), override_settings(MIDDLEWARE=["django.middleware.common.CommonMiddleware", "usagerail.django.UsageRailMiddleware"], APPEND_SLASH=True):
        response = Client().get("/v1/users/7", headers=bearer(key))
        assert response.status_code == 301
        # The reservation came back: the server only grants 5, so the same lease can admit the next request needing 5
        assert Client().get("/v1/users/7/", headers=bearer(key)).status_code == 200
    engine.flush()
    assert summary(transport) == [("GET", "_unmatched", "4xx", 0), ("GET", "/v1/users/<int:pk>/", "2xx", 5)]


def test_under_an_asgi_server_the_wrapper_reports_what_is_left_when_the_server_shuts_down():
    import asyncio

    from usagerail.django import asgi

    engine, transport, key, _ = backend()
    passed_through = []

    async def django_app(scope, receive, send):
        passed_through.append(scope["type"])

    with client_for(engine, transport):
        assert Client().get("/v1/users/7/", headers=bearer(key)).status_code == 200
    assert transport.ingested == []
    application = asgi(django_app)

    async def lifespan():
        incoming = asyncio.Queue()
        sent = []

        async def send(message):
            sent.append(message["type"])
            # The shutdown only counts as done once the server receives "shutdown complete"
            if message["type"] == "lifespan.shutdown.complete":
                summaries.append(summary(transport))

        await incoming.put({"type": "lifespan.startup"})
        await incoming.put({"type": "lifespan.shutdown"})
        await application({"type": "lifespan"}, incoming.get, send)
        await application({"type": "http"}, incoming.get, send)
        return sent

    summaries = []
    assert asyncio.run(lifespan()) == ["lifespan.startup.complete", "lifespan.shutdown.complete"]
    # The remaining usage was reported before telling the server it's done; HTTP requests are passed to Django unchanged
    assert summaries == [[("GET", "/v1/users/<int:pk>/", "2xx", 1)]]
    assert passed_through == ["http"]


def test_the_asgi_wrapper_closes_its_engines_at_the_same_time():
    import asyncio
    import threading
    import time
    from concurrent.futures import Future

    from usagerail import django as django_adapter

    class SlowEngine:
        def close(self):
            done: Future = Future()
            threading.Timer(0.3, lambda: done.set_result(None)).start()
            return done

    engines = [SlowEngine(), SlowEngine()]
    for engine in engines:
        django_adapter._middleware_engines.add(engine)  # noqa: SLF001

    async def shutdown():
        incoming = asyncio.Queue()
        await incoming.put({"type": "lifespan.shutdown"})
        sent = []

        async def send(message):
            sent.append(message["type"])

        started = time.monotonic()
        await django_adapter.asgi(lambda scope, receive, send: None)({"type": "lifespan"}, incoming.get, send)
        return sent, time.monotonic() - started

    sent, elapsed = asyncio.run(shutdown())
    assert sent == ["lifespan.shutdown.complete"]
    # Each takes 0.3 seconds: closing both at once takes a little over 0.3 seconds, one after the other 0.6
    assert elapsed < 0.5, elapsed
