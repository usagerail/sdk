import asyncio
import json
import threading
from concurrent.futures import Future

from adapter_helpers import backend, bearer, stop_backend, summary
from helpers import app_settings, sync_page
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Host, Mount, Route, Router

from usagerail.fastapi import UsageRailMiddleware


def test_route_templates_track_rejections_and_unmatched_paths():
    engine, transport, key, limited = backend()
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)
    v1 = APIRouter(prefix="/v1")

    @v1.get("/users/{user_id}")
    def user(user_id: str):
        return {"id": user_id}

    @v1.post("/generate", status_code=201)
    async def generate(request: Request):
        request.state.usagerail.track(42)
        return {"ok": True}

    app.include_router(v1)
    client = TestClient(app)
    assert client.get("/v1/users/7?expand=1", headers=bearer(key)).json() == {"id": "7"}
    assert client.post("/v1/generate", headers={"x-api-key": key}).status_code == 201
    invalid = client.get("/v1/users/7", headers=bearer("acme_test_nope"))
    assert invalid.status_code == 401
    assert invalid.json() == {"error": "invalid_api_key"}
    assert client.get("/nowhere", headers=bearer(key)).status_code == 404
    assert client.get("/v1/users/1", headers=bearer(limited)).status_code == 200
    too_many = client.get("/v1/users/1", headers=bearer(limited))
    assert too_many.status_code == 429
    assert too_many.headers["retry-after"] == "1"
    assert too_many.headers["ratelimit-policy"]
    assert too_many.json()["error"] == "rate_limited"
    engine.flush()
    # The middleware decides before routing, so rejected requests have no template and are recorded as _unmatched (like Express)
    assert summary(transport) == [
        ("GET", "/v1/users/{user_id}", "2xx", 1),
        ("POST", "/v1/generate", "2xx", 42),
        ("GET", "_unmatched", "sdk_401", 0),
        ("GET", "_unmatched", "4xx", 0),
        ("GET", "/v1/users/{user_id}", "2xx", 1),
        ("GET", "_unmatched", "sdk_429", 0),
    ]


def test_routes_under_a_starlette_mount_carry_the_mount_point():
    engine, transport, key, _ = backend()

    async def item(request):
        return JSONResponse({"id": request.path_params["item_id"]})

    app = Starlette(routes=[Mount("/v2", routes=[Route("/items/{item_id}", item)])])
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)
    assert TestClient(app).get("/v2/items/9", headers=bearer(key)).json() == {"id": "9"}
    engine.flush()
    assert summary(transport) == [("GET", "/v2/items/{item_id}", "2xx", 1)]


def test_an_sse_response_is_metered_once_as_soon_as_it_starts_streaming():
    engine, transport, key, _ = backend()
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)
    seen_while_streaming = []

    @app.get("/v1/events")
    async def events(request: Request):
        async def stream():
            yield "data: hello\n\n"
            # The stream is still open: it has already counted once (long-lived connections count as ws_open; units are added at the end)
            await asyncio.sleep(0)
            engine.flush()
            seen_while_streaming.extend((row["result_type"], row["count"], row["units"]) for row in transport.rows())
            request.state.usagerail.track(7)
            yield "data: bye\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream")

    response = TestClient(app).get("/v1/events", headers=bearer(key))
    assert response.text == "data: hello\n\ndata: bye\n\n"
    engine.flush()
    assert seen_while_streaming == [("ws_open", 1, 0)]
    # Units are added at the end: in an app metering units, track() wins
    assert [(row["route"], row["result_type"], row["count"], row["units"]) for row in transport.rows()] == [
        ("/v1/events", "ws_open", 1, 0),
        ("/v1/events", "ws_open", 0, 7),
    ]


def test_a_weighted_route_reserves_its_weight_before_routing():
    # A request needs 10, the server has only 5 left: block it, don't let it in as 1
    engine, _, key = stop_backend([{"method": "POST", "route": "/v1/images/{image_id}", "weight": 10, "reserve": 10}])
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.post("/v1/images/{image_id}")
    def image(image_id: str):
        return {"ok": True}

    client = TestClient(app)
    assert client.post("/v1/images/3", headers=bearer(key)).status_code == 429
    assert client.post("/v1/other", headers=bearer(key)).status_code == 404


def test_custom_auth_reads_the_consumer_from_the_request():
    engine, transport, _, _ = backend(auth="custom", consumer=lambda request: request.headers.get("x-user"))
    transport.on_sync = lambda cursor: sync_page(app=app_settings(hash_key=None))
    engine.sync()
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="custom", engine=engine)

    @app.get("/me")
    def me():
        return {"ok": True}

    client = TestClient(app)
    assert client.get("/me", headers={"x-user": "user-1"}).status_code == 200
    assert client.get("/me").status_code == 200
    engine.flush()
    assert sorted((row["consumer"], row["consumer_kind"]) for row in transport.rows()) == [("", "anonymous"), ("user-1", "external")]


def test_on_reject_writes_its_own_response():
    engine, _, _, _ = backend()
    app = FastAPI()
    app.add_middleware(
        UsageRailMiddleware,
        app_key="unused",
        auth="api-key",
        engine=engine,
        on_reject=lambda rejection, request: JSONResponse({"message": f"no: {rejection.error}"}, status_code=rejection.status),
    )
    response = TestClient(app).get("/anything")
    assert (response.status_code, response.json()) == (401, {"message": "no: invalid_api_key"})


def test_an_exception_in_the_handler_is_recorded_as_5xx():
    engine, transport, key, _ = backend()
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.get("/boom")
    def boom():
        raise RuntimeError("boom")

    assert TestClient(app, raise_server_exceptions=False).get("/boom", headers=bearer(key)).status_code == 500
    engine.flush()
    assert summary(transport) == [("GET", "/boom", "5xx", 0)]


def test_shutting_the_app_down_reports_what_is_left():
    engine, transport, key, _ = backend()
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.get("/x")
    def x():
        return {"ok": True}

    with TestClient(app) as client:
        assert client.get("/x", headers=bearer(key)).status_code == 200
        assert transport.ingested == []
    # lifespan shutdown: the remaining usage is reported before exit
    assert summary(transport) == [("GET", "/x", "2xx", 1)]


def test_a_lease_answered_later_from_another_thread_wakes_the_waiting_request():
    # A real transport answers on a network thread: await waits for it without blocking the event loop
    engine, transport, key = stop_backend([])
    answer_with = transport.on_leases

    def later(request):
        answer: Future = Future()
        threading.Timer(0.05, lambda: answer.set_result(answer_with(request))).start()
        return answer

    transport.on_leases = later
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.get("/x")
    async def x():
        return {"ok": True}

    assert TestClient(app).get("/x", headers=bearer(key)).status_code == 200
    assert len(transport.lease_calls) == 1


# ---- Found in review: middleware inside a mounted sub-app; the client leaves while waiting for a lease ----
def test_middleware_inside_a_mounted_app_reserves_by_the_full_path():
    # The seller configured the full path /v1/generate; the middleware sits in a sub-app mounted at /v1 and must match the full path at the start too
    engine, _, key = stop_backend([{"method": "POST", "route": "/v1/generate", "weight": 10, "reserve": 10}])
    sub = FastAPI()
    sub.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)
    handled = []

    @sub.post("/generate")
    def generate():
        handled.append(1)
        return {"ok": True}

    app = Starlette(routes=[Mount("/v1", app=sub)])
    assert TestClient(app).post("/v1/generate", headers=bearer(key)).status_code == 429
    assert handled == []


def test_a_client_that_leaves_while_the_lease_is_on_its_way_is_settled_as_aborted():
    engine, transport, key = stop_backend([])
    answer_with = transport.on_leases
    waiting = []

    def later(request):
        answer: Future = Future()
        waiting.append((answer, request))
        return answer

    transport.on_leases = later
    handled = []

    async def inner(scope, receive, send):
        handled.append(scope["path"])

    middleware = UsageRailMiddleware(inner, app_key="unused", auth="api-key", engine=engine)
    scope = {"type": "http", "method": "GET", "path": "/x", "root_path": "", "query_string": b"", "headers": [(b"authorization", f"Bearer {key}".encode())]}

    sent = []

    async def receive():
        # Like a real server: after the body is sent, the next message only comes when the client disconnects (which it never does here)
        if not sent:
            sent.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        pass

    async def leave_while_waiting():
        task = asyncio.ensure_future(middleware(scope, receive, send))
        await asyncio.sleep(0.01)
        assert len(waiting) == 1, "the request should be waiting for its first lease"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(leave_while_waiting())
    # The lease only arrives now: this request is recorded as aborted, the reservation refunded, and the handler never ran
    answer, request = waiting[0]
    answer.set_result(answer_with(request))
    engine.flush()
    assert handled == []
    assert [row[2] for row in summary(transport)] == ["aborted"]


def test_a_disconnect_message_while_the_lease_is_on_its_way_is_settled_as_aborted():
    # uvicorn only sends http.disconnect when the client disconnects, without cancelling the task: still skip the handler and settle as aborted
    engine, transport, key = stop_backend([])
    answer_with = transport.on_leases
    waiting = []

    def later(request):
        answer: Future = Future()
        waiting.append((answer, request))
        return answer

    transport.on_leases = later
    handled = []

    async def inner(scope, receive, send):
        handled.append(scope["path"])

    middleware = UsageRailMiddleware(inner, app_key="unused", auth="api-key", engine=engine)
    scope = {"type": "http", "method": "POST", "path": "/x", "root_path": "", "query_string": b"", "headers": [(b"authorization", f"Bearer {key}".encode())]}

    async def scenario():
        gone = asyncio.Event()
        sent = []

        async def receive():
            if not sent:
                sent.append(True)
                return {"type": "http.request", "body": b"payload", "more_body": False}
            await gone.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            pass

        task = asyncio.ensure_future(middleware(scope, receive, send))
        await asyncio.sleep(0.01)
        assert len(waiting) == 1, "the request should be waiting for its first lease"
        gone.set()
        await asyncio.wait_for(task, 5)

    asyncio.run(scenario())
    answer, request = waiting[0]
    answer.set_result(answer_with(request))
    engine.flush()
    assert handled == []
    assert [row[2] for row in summary(transport)] == ["aborted"]


def test_the_request_body_read_while_waiting_for_the_lease_still_reaches_the_handler():
    engine, transport, key = stop_backend([])
    answer_with = transport.on_leases
    transport.on_leases = lambda request: _later(answer_with(request))
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.post("/echo")
    async def echo(request: Request):
        return {"body": (await request.body()).decode()}

    response = TestClient(app).post("/echo", content=b"hello", headers=bearer(key))
    assert response.json() == {"body": "hello"}


def _later(value):
    answer: Future = Future()
    threading.Timer(0.05, lambda: answer.set_result(value)).start()
    return answer


# ---- Found in review (round two): reported templates keep the path converter; parameterized mounts report the template, not the customer's ID ----
def test_a_path_converter_route_is_reported_with_its_converter():
    engine, transport, key, _ = backend()
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.get("/files/{rest:path}")
    def files(rest: str):
        return {"rest": rest}

    assert TestClient(app).get("/files/foo/bar", headers=bearer(key)).json() == {"rest": "foo/bar"}
    engine.flush()
    assert summary(transport) == [("GET", "/files/{rest:path}", "2xx", 1)]


def test_a_path_converter_template_reserves_its_weight_for_any_depth():
    engine, _, key = stop_backend([{"method": "GET", "route": "/files/{rest:path}", "weight": 10, "reserve": 10}])
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.get("/files/{rest:path}")
    def files(rest: str):
        return {"rest": rest}

    assert TestClient(app).get("/files/foo/bar", headers=bearer(key)).status_code == 429


def test_parameterized_mounts_report_their_template_not_the_customer_id():
    engine, transport, key, _ = backend()

    async def item(request):
        return JSONResponse({"id": request.path_params["item_id"]})

    app = Starlette(routes=[Mount("/accounts/{account_id}", routes=[Route("/items/{item_id}", item)])])
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)
    client = TestClient(app)
    for account in ("acct-1", "acct-2"):
        assert client.get(f"/accounts/{account}/items/9", headers=bearer(key)).json() == {"id": "9"}
    engine.flush()
    assert summary(transport) == [("GET", "/accounts/{account_id}/items/{item_id}", "2xx", 2)]


# ---- Found in review (round four): mount templates come from the declared Mounts, without matching parameter values against path segments ----
def test_mount_templates_come_from_the_declared_mounts_with_converters_nesting_and_hosts():
    engine, transport, key, _ = backend()

    async def item(request):
        return JSONResponse({"id": request.path_params["item_id"]})

    projects = Mount("/projects/{project_id}", routes=[Route("/items/{item_id}", item)])
    app = Starlette(
        routes=[
            Host("{tenant}.example.com", app=Router(routes=[Mount("/v1/{region}", routes=[Route("/items/{item_id}", item)])])),
            Mount("/accounts/{account_id:int}", routes=[projects]),
            Mount("/teams/{team_id}", routes=[Route("/items/{item_id}", item)]),
        ]
    )
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)
    client = TestClient(app)
    # The converter rewrote the value (001 becomes 1), so it doesn't match the original text in the path
    assert client.get("/accounts/001/projects/7/items/9", headers=bearer(key)).status_code == 200
    # The ID happens to equal a static segment
    assert client.get("/teams/teams/items/9", headers=bearer(key)).status_code == 200
    assert client.get("/v1/eu/items/9", headers={**bearer(key), "host": "acme.example.com"}).status_code == 200
    engine.flush()
    assert summary(transport) == [
        ("GET", "/accounts/{account_id:int}/projects/{project_id}/items/{item_id}", "2xx", 1),
        ("GET", "/teams/{team_id}/items/{item_id}", "2xx", 1),
        ("GET", "/v1/{region}/items/{item_id}", "2xx", 1),
    ]


# ---- Billing only trusts the template the framework reports ----
def test_a_route_the_seller_did_not_price_is_billed_1_even_when_a_priced_template_matched_its_path_at_the_start():
    # The seller configured /v1/{rest:path}, the app handles the request with /v1/items/{item_id}: at the start it reserves 3 from the lease by the former, but settlement only trusts the framework's template and counts 1
    engine, transport, key = stop_backend([{"method": "ALL", "route": "/v1/{rest:path}", "weight": 3, "reserve": 3}])
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.get("/v1/items/{item_id}")
    def item(item_id: str):
        return {"id": item_id}

    assert TestClient(app).get("/v1/items/7", headers=bearer(key)).status_code == 200
    engine.flush()
    assert summary(transport) == [("GET", "/v1/items/{item_id}", "2xx", 1)]


# ---- Found in review (round five): while waiting for a decision, at most 64 KiB of request body is read ahead ----
def test_while_admission_waits_at_most_64_kib_of_a_large_upload_is_read_ahead():
    engine, transport, key = stop_backend([])
    answer: Future = Future()
    answer_with = transport.on_leases
    transport.on_leases = lambda request: answer
    app = FastAPI()
    app.add_middleware(UsageRailMiddleware, app_key="unused", auth="api-key", engine=engine)

    @app.post("/v1/upload")
    async def upload(request: Request):
        return {"size": len(await request.body())}

    chunk = b"x" * (1024 * 1024)
    pulled = {"chunks": 0}

    async def receive():
        if pulled["chunks"] < 10:
            pulled["chunks"] += 1
            return {"type": "http.request", "body": chunk, "more_body": pulled["chunks"] < 10}
        await asyncio.sleep(3600)
        return {"type": "http.disconnect"}

    sent = []

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "POST", "scheme": "http", "path": "/v1/upload", "raw_path": b"/v1/upload", "root_path": "", "query_string": b"", "headers": [(b"authorization", f"Bearer {key}".encode())], "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 80)}

    async def scenario():
        call = asyncio.ensure_future(app(scope, receive, send))
        for _ in range(50):
            await asyncio.sleep(0)
        # No decision yet: a single 1 MiB chunk is over the limit, and the other 9 chunks haven't been read
        assert pulled["chunks"] == 1
        answer.set_result(answer_with({"requests": [{"consumer": "cons_stop", "reserve": 1, "quota_version": 1}]}))
        await asyncio.wait_for(call, 10)

    asyncio.run(scenario())
    assert pulled["chunks"] == 10
    body = b"".join(message.get("body", b"") for message in sent if message["type"] == "http.response.body")
    assert json.loads(body) == {"size": 10 * len(chunk)}
