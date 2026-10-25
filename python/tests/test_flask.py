import pytest
from adapter_helpers import backend, bearer, stop_backend, summary
from flask import Blueprint, Flask, Response, g, jsonify
from flask.testing import FlaskClient

from usagerail.flask import UsageRail


class ClosingClient(FlaskClient):
    """A real WSGI server always calls close() after sending the response (PEP 3333), which is when usage is settled; Werkzeug's test client doesn't by default, so this one closes after reading"""

    def open(self, *args, buffered=True, **kwargs):
        return super().open(*args, buffered=buffered, **kwargs)


def make_app(engine, **extension_options):
    app = Flask(__name__)
    app.test_client_class = ClosingClient
    UsageRail(app, engine=engine, **extension_options)
    return app


def test_route_templates_track_rejections_and_unmatched_paths():
    engine, transport, key, limited = backend()
    app = make_app(engine)
    v1 = Blueprint("v1", __name__, url_prefix="/v1")

    @v1.get("/users/<int:user_id>")
    def user(user_id):
        return {"id": user_id}

    @v1.post("/generate")
    def generate():
        g.usagerail.track(42)
        return {"ok": True}, 201

    app.register_blueprint(v1)
    client = app.test_client()
    assert client.get("/v1/users/7?expand=1", headers=bearer(key)).json == {"id": 7}
    assert client.post("/v1/generate", headers={"X-Api-Key": key}).status_code == 201
    invalid = client.get("/v1/users/7", headers=bearer("acme_test_nope"))
    assert invalid.status_code == 401
    assert invalid.json == {"error": "invalid_api_key"}
    assert client.get("/nowhere", headers=bearer(key)).status_code == 404
    assert client.get("/v1/users/1", headers=bearer(limited)).status_code == 200
    too_many = client.get("/v1/users/1", headers=bearer(limited))
    assert too_many.status_code == 429
    assert too_many.headers["Retry-After"] == "1"
    assert too_many.headers["RateLimit-Policy"]
    assert too_many.json["error"] == "rate_limited"
    engine.flush()
    # Flask has routed before before_request, so even requests the SDK rejects get a template (like Fastify)
    assert summary(transport) == [
        ("GET", "/v1/users/<int:user_id>", "2xx", 1),
        ("POST", "/v1/generate", "2xx", 42),
        ("GET", "/v1/users/<int:user_id>", "sdk_401", 0),
        ("GET", "_unmatched", "4xx", 0),
        ("GET", "/v1/users/<int:user_id>", "2xx", 1),
        ("GET", "/v1/users/<int:user_id>", "sdk_429", 0),
    ]


def test_an_sse_response_is_metered_once_as_soon_as_it_starts_streaming():
    engine, transport, key, _ = backend()
    app = make_app(engine)
    seen_while_streaming = []

    @app.get("/v1/events")
    def events():
        handle = g.usagerail

        def stream():
            yield "data: hello\n\n"
            engine.flush()
            seen_while_streaming.extend((row["result_type"], row["count"], row["units"]) for row in transport.rows())
            handle.track(7)
            yield "data: bye\n\n"

        return Response(stream(), mimetype="text/event-stream")

    response = app.test_client().get("/v1/events", headers=bearer(key), buffered=False)
    assert response.get_data(as_text=True) == "data: hello\n\ndata: bye\n\n"
    response.close()
    engine.flush()
    # The stream is still open: it has already counted once (long-lived connections count as ws_open), and units are added by track() when it ends
    assert seen_while_streaming == [("ws_open", 1, 0)]
    assert [(row["route"], row["result_type"], row["count"], row["units"]) for row in transport.rows()] == [
        ("/v1/events", "ws_open", 1, 0),
        ("/v1/events", "ws_open", 0, 7),
    ]


def test_a_weighted_route_reserves_its_template_weight():
    # A request needs 10, the server has only 5 left: block it, don't let it in as 1. Flask provides the template at the start, so changes in casing or encoding all follow it
    engine, _, key = stop_backend([{"method": "POST", "route": "/v1/images/<image_id>", "weight": 10, "reserve": 10}])
    app = make_app(engine)

    @app.post("/v1/images/<image_id>")
    def image(image_id):
        return {"ok": True}

    client = app.test_client()
    assert client.post("/v1/images/3", headers=bearer(key)).status_code == 429
    assert client.post("/v1/images/%33", headers=bearer(key)).status_code == 429


def test_an_unhandled_exception_is_recorded_as_5xx():
    engine, transport, key, _ = backend()
    app = make_app(engine)

    @app.get("/boom")
    def boom():
        raise RuntimeError("boom")

    assert app.test_client().get("/boom", headers=bearer(key)).status_code == 500
    # In tests the exception propagates all the way out (PROPAGATE_EXCEPTIONS): after_request doesn't run, and teardown records 500
    app.testing = True
    try:
        app.test_client().get("/boom", headers=bearer(key))
    except RuntimeError:
        pass
    engine.flush()
    assert summary(transport) == [("GET", "/boom", "5xx", 0)]
    assert transport.rows()[0]["count"] == 2


def test_on_reject_writes_its_own_response():
    engine, _, _, _ = backend()
    app = make_app(engine, on_reject=lambda rejection, request: (jsonify(message=f"no: {rejection.error}"), rejection.status))
    response = app.test_client().get("/anything")
    assert (response.status_code, response.json) == (401, {"message": "no: invalid_api_key"})


def test_custom_auth_reads_the_consumer_from_the_request():
    engine, transport, _, _ = backend(auth="custom", consumer=lambda request: request.headers.get("X-User"))
    app = make_app(engine)

    @app.get("/me")
    def me():
        return {"ok": True}

    client = app.test_client()
    assert client.get("/me", headers={"X-User": "user-1"}).status_code == 200
    assert client.get("/me").status_code == 200
    engine.flush()
    assert sorted(row["consumer_kind"] for row in transport.rows()) == ["anonymous", "external"]


# ---- Found in review: send_file() passes the file straight through, bypassing Response.close() ----
def test_a_file_download_is_metered_when_the_server_closes_it():
    import io

    from flask import send_file

    engine, transport, key, _ = backend()
    app = make_app(engine)

    @app.get("/v1/report")
    def report():
        return send_file(io.BytesIO(b"report body"), mimetype="application/octet-stream")

    response = app.test_client().get("/v1/report", headers=bearer(key))
    assert response.status_code == 200
    assert response.data == b"report body"
    engine.flush()
    assert summary(transport) == [("GET", "/v1/report", "2xx", 1)]


# ---- Found in review (round four): the final status code is what's recorded; a trailing-slash redirect isn't billed by the guessed template ----
def test_an_after_request_hook_that_runs_later_and_changes_the_status_decides_what_is_recorded():
    engine, transport, key, _ = backend()
    app = Flask(__name__)
    app.test_client_class = ClosingClient

    # after_request functions registered earlier run later (Flask runs them in reverse), changing the status after UsageRail's
    @app.after_request
    def forbid(response):
        response.status_code = 403
        return response

    UsageRail(app, engine=engine)

    @app.get("/v1/items")
    def items():
        return {"ok": True}

    assert app.test_client().get("/v1/items", headers=bearer(key)).status_code == 403
    engine.flush()
    assert summary(transport) == [("GET", "/v1/items", "4xx", 0)]


def test_a_trailing_slash_redirect_is_unmatched_not_the_priced_template_its_path_looked_like():
    # The seller priced /expensive/ at 10. For /expensive, Flask answers 308 to add the slash and never reaches that route: recorded as _unmatched, counting 1
    engine, transport, key, _ = backend([{"method": "GET", "route": "/expensive/", "weight": 10, "reserve": 10}])
    app = make_app(engine)

    @app.get("/expensive/")
    def expensive():
        return {"ok": True}

    assert app.test_client().get("/expensive", headers=bearer(key)).status_code == 308
    engine.flush()
    assert summary(transport) == [("GET", "_unmatched", "3xx", 1)]



def test_usage_is_settled_even_when_closing_the_response_body_fails():
    engine, transport, key, _ = backend()
    app = make_app(engine)

    class Body:
        """The response body's own close() raises (say, cleaning up a temp file fails)"""

        def __iter__(self):
            yield b"x"

        def close(self):
            raise RuntimeError("cleanup failed")

    @app.get("/v1/stream")
    def stream():
        return Response(Body())

    with pytest.raises(RuntimeError):
        app.test_client().get("/v1/stream", headers=bearer(key))
    engine.flush()
    assert summary(transport) == [("GET", "/v1/stream", "2xx", 1)]
