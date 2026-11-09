---
name: add-usagerail
description: Add API keys, per-customer rate limits, quotas and usage metering to a Node.js or Python API server with the UsageRail SDK. Use when someone wants to sell access to their API, charge by usage through their own Stripe account, or keep one customer from using up their API.
metadata:
  tags: "api, api-keys, rate-limiting, usage-billing, stripe, fastapi, express, django, flask, hono"
---

# Add UsageRail to an API

UsageRail's SDK checks API keys, applies rate limits and quotas, and counts usage per customer inside the user's own API server. Requests never pass through UsageRail. Counts go to UsageRail in the background: every 60 seconds on a long-running server, and after each response in Next.js.

## Before you change anything

1. Find the framework. The SDKs cover Express, Hono, Fastify and Next.js on Node.js 22+, and FastAPI, Starlette, Flask and Django on Python 3.10+. For other languages, use the HTTP check endpoint described at https://usagerail.com/docs. It adds one network call per request.
2. Ask how callers authenticate today. With no keys yet, use `auth: "api-key"` and UsageRail issues the keys. With existing keys or logins, use `auth: "custom"` with a `consumer` function. In Node.js it returns `{ id }` for the caller, or `null` for anonymous traffic. In Python it returns the customer's ID as a string, or `None`. A bare string returned in Node.js is counted as anonymous. Return an identity the app's own code has verified, never a value the caller can set, such as a plain header. UsageRail has to run after that authentication: in Express, `app.use(usageRail(...))` goes after the auth middleware. In FastAPI the identity must come from an authentication middleware, not `Depends(...)`, and since FastAPI runs the middleware added last first, add the auth middleware after the line that adds UsageRail. In Django, list UsageRail after `AuthenticationMiddleware`. Django REST framework token or JWT authentication runs inside the view, after all middleware, so a consumer that reads `request.user` sees anonymous traffic: verify the token in the consumer function or in an earlier middleware. In Flask, register the app's own `before_request` hook first.
3. List the routes that must answer without a UsageRail key, such as health checks, webhook receivers and browsers' CORS preflight (`OPTIONS`) requests. With `auth: "api-key"`, any request UsageRail sees without a valid key gets a 401, which can fail the hosting platform's health check and stop webhook deliveries, so keep those requests away from it. In Express, mount it on the paid API's path, `app.use("/v1", usageRail(...))`, and register the CORS middleware before it. In FastAPI, add `CORSMiddleware` after the line that adds UsageRail, so it runs first. In Django, list the CORS middleware above UsageRail. In Flask, register a `before_request` hook before UsageRail that returns `app.make_default_options_response()` for `OPTIONS` requests. The Python adapters have no setting that skips a route, so for health checks and webhooks in FastAPI, Flask and Django, add the few lines shown in that framework's section below. Before deploying, call each open route without a key and check that it still answers, then call a paid route without a key and check for the 401.
4. The user needs an app key from https://app.usagerail.com. Read it from the `USAGERAIL_APP_KEY` environment variable. Never write the key into source code, and never ask the user to paste it into the chat.

## Wire it in

Every request counts as 1 by default. The `track()` calls below only change that once the app counts units, which is set in the console under Settings > What counts as usage. While the app counts requests, `track()` does nothing.

When the app counts units, each request reserves units from the customer's quota before the handler runs. A route listed under Settings > Route weights reserves the larger of its weight and its "Reserve with track()" value; any other route reserves 1. So if a route that counts tokens isn't listed, a customer on a plan that stops at the limit, with 5 units left, can still start a request that uses 1,000 tokens and go over by the difference. For every route that calls `track()`, set Reserve with track() to the most units one request on that route can use. Then give a test key a plan with only a few units left, send several requests at once, and check that they get a 429 instead of going over.

### FastAPI or Starlette

```bash
pip install usagerail
```

```python
import os

from fastapi import FastAPI, Request
from usagerail.fastapi import UsageRailMiddleware

app = FastAPI()
app.add_middleware(UsageRailMiddleware, app_key=os.environ["USAGERAIL_APP_KEY"], auth="api-key")


@app.post("/v1/generate")
async def generate(request: Request):
    result = await run_model(await request.json())
    request.state.usagerail.track(result.tokens)  # optional: count tokens instead of requests
    return result.body
```

To leave health checks and webhooks open, wrap the middleware so those exact paths skip it, and use this `add_middleware` line in place of the one above. Don't move the paid routes into a mounted sub-application instead: a mounted app gets no lifespan events, so the counts left at shutdown are never sent.

```python
OPEN_PATHS = {"/healthz", "/webhooks/stripe"}


def route_path(scope):
    # The path the router matches: like Starlette, drop the server's root_path (set behind a proxy prefix) first
    path, root = scope["path"], scope.get("root_path", "")
    if root and path.startswith(root) and path[len(root):][:1] in ("", "/"):
        return path[len(root):]
    return path


class UsageRailExceptOpenPaths:
    def __init__(self, app, **options):
        self.open_app = app
        self.rail = UsageRailMiddleware(app, **options)

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and route_path(scope) in OPEN_PATHS:
            await self.open_app(scope, receive, send)
        else:
            # Everything else, lifespan included, goes through UsageRail, so shutdown still sends the counts
            await self.rail(scope, receive, send)


app.add_middleware(UsageRailExceptOpenPaths, app_key=os.environ["USAGERAIL_APP_KEY"], auth="api-key")
```

Paths are compared exactly, so `/healthz/` still asks for a key.

### Flask

```python
import os

from flask import Flask
from usagerail.flask import UsageRail

app = Flask(__name__)
UsageRail(app, app_key=os.environ["USAGERAIL_APP_KEY"], auth="api-key")
```

Inside a route, `g.usagerail.track(n)` counts custom units.

The extension checks every request the Flask app handles. To leave health checks and webhooks open, register them on a second Flask app and send those exact paths to it, after the `UsageRail(app, ...)` line:

```python
OPEN_PATHS = {"/healthz", "/webhooks/stripe"}
open_app = Flask("open_routes")  # register the health check and webhook routes on this app
paid_api = app.wsgi_app


def route_open_paths(environ, start_response):
    target = open_app if environ.get("PATH_INFO") in OPEN_PATHS else paid_api
    return target(environ, start_response)


app.wsgi_app = route_open_paths
```

The server still runs `app`. Routes on `open_app` don't share the main app's config or extensions, so set up whatever they use there too, such as the database.

### Django

```python
# settings.py
import os

MIDDLEWARE = [
    # ...
    "usagerail.django.UsageRailMiddleware",
]
USAGERAIL = {"app_key": os.environ["USAGERAIL_APP_KEY"], "auth": "api-key"}
```

Inside a view, `request.usagerail.track(n)` counts custom units.

To leave health checks and webhooks open, subclass the middleware so those exact paths skip it, and list the subclass in `MIDDLEWARE` in place of `"usagerail.django.UsageRailMiddleware"`:

```python
# myapp/middleware.py
from usagerail.django import UsageRailMiddleware

OPEN_PATHS = {"/healthz/", "/webhooks/stripe/"}


class UsageRailExceptOpenPaths(UsageRailMiddleware):
    def __call__(self, request):
        if request.path_info in OPEN_PATHS:
            return self.get_response(request)
        return super().__call__(request)
```

Under uvicorn or another ASGI server, wrap the application in `asgi.py`, or every restart loses the counts that hadn't gone out yet:

```python
import os

from django.core.asgi import get_asgi_application
from usagerail.django import asgi

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

application = asgi(get_asgi_application())
```

### Express

```bash
npm install @usagerail/node
```

```js
import express from "express";
import { usageRail } from "@usagerail/node/express";

const app = express();

app.use(usageRail({
  appKey: process.env.USAGERAIL_APP_KEY,
  auth: "api-key",
}));

app.post("/v1/generate", express.json(), async (req, res) => {
  const result = await generate(req.body);
  req.usageRail.track(result.tokens); // optional: count tokens
  res.json(result);
});
```

Hono and Fastify have their own entry points: `@usagerail/node/hono` and `@usagerail/node/fastify`. Hono on Node.js doesn't flush on its own: also call `flushOnExit(rail.engine)` from `@usagerail/node/node`, or counts not yet sent are lost when the process stops. Follow the framework's page in the docs for the full setup.

On SIGTERM the Express middleware sends what's left, waiting at most 5 seconds, then lets the process exit. If the app handles SIGTERM itself, keep the value `usageRail()` returned and finish the app's own shutdown with `await middleware.engine.close()`.

## How callers send their key

In the `Authorization: Bearer <key>` header, or in `x-api-key` when there's no bearer token.

## What rejected requests look like

| Status | Meaning |
|---|---|
| 401 | Unknown or revoked key |
| 402 | Prepaid credit used up |
| 403 | Suspended customer |
| 429 | Rate limit or quota reached |
| 503 | UsageRail unreachable, and the app is set to refuse |

Update the API's own docs and client error handling for these.

## Tell the user before they deploy

- Rate limits and quotas come from plans, set up in the console. A customer with no plan has no rate limit or quota. Make default on a free plan only applies to customers created after that: existing customers keep the plan they had, or stay without one. So make the default plan before creating customers, and move any existing ones onto a plan with limits by hand, on the customer's page in the console or with `POST /v1/consumers/{id}/plan`. Once that's done, go over a limit with a test key and check for the 429.
- Rate limits are counted per server instance by default.
- The Express middleware and the Python SDK don't check or count WebSocket connections. Hono counts a successful handshake once, and so does Fastify with `@fastify/websocket`.
- On a long-running server, counts are sent every 60 seconds. A process killed with `kill -9` or by running out of memory loses the counts it hadn't sent, and they're never billed.
- With Express and Fastify, and Hono with `flushOnExit`, the SDK sends the remaining counts on SIGTERM and when the process exits on its own. SIGINT (Ctrl+C) and a direct `process.exit()` skip that, so an app that stops another way should `await engine.close()` before it exits.
- Next.js has no background timer or exit hook: pass `waitUntil: after` from `next/server` (or `waitUntil` from `@vercel/functions`) so the counts go out after each response. See https://usagerail.com/docs/nextjs.
- In Python, FastAPI sends them during lifespan shutdown, and Flask and Django when the process exits normally, as under gunicorn. Django under uvicorn or another ASGI server needs the `asgi.py` wrapper above.
- To charge customers, the user connects their own Stripe account. Money goes straight there, and UsageRail is not a merchant of record.

Setup for each framework, options and known limitations: [UsageRail docs](https://usagerail.com/docs)
