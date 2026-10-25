# usagerail

The UsageRail SDK for Python. It checks API keys, applies rate limits and quotas, and counts usage per customer inside your own process, then reports the counts to UsageRail in the background. Requests never pass through us.

It needs Python 3.10 or newer and has no dependencies of its own. Versions start at 0.x, so names may still change between minor versions.

```bash
pip install usagerail
```

Get the app key from the console at https://app.usagerail.com. Setup for each framework, options and known limitations are in the docs at https://usagerail.com/docs.

## FastAPI and Starlette

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

## Flask

```python
import os

from flask import Flask, g, request
from usagerail.flask import UsageRail

app = Flask(__name__)
UsageRail(app, app_key=os.environ["USAGERAIL_APP_KEY"], auth="api-key")


@app.post("/v1/generate")
def generate():
    result = run_model(request.json)
    g.usagerail.track(result.tokens)
    return result.body
```

## Django

```python
# settings.py
MIDDLEWARE = [
    # ...
    "usagerail.django.UsageRailMiddleware",
]
USAGERAIL = {"app_key": os.environ["USAGERAIL_APP_KEY"], "auth": "api-key"}

# views.py
def generate(request):
    result = run_model(request.body)
    request.usagerail.track(result.tokens)
    return JsonResponse(result.body)
```

## Your own authentication

If your API already has its own keys or logins, pass `auth="custom"` and a `consumer` function. It gets the framework's request and returns your customer's ID, or `None` for anonymous traffic. Return an identity your own code has verified, never a value the caller can set, such as a plain header:

```python
app.add_middleware(
    UsageRailMiddleware,
    app_key=os.environ["USAGERAIL_APP_KEY"],
    auth="custom",
    consumer=lambda request: request.user.identity if request.user.is_authenticated else None,
)
```

The function runs before your route handlers. In FastAPI that is before `Depends(...)`, so the identity has to come from an authentication middleware that runs first. FastAPI runs the middleware added last first, so call `app.add_middleware()` for your authentication after the line that adds UsageRail. In Django, list UsageRail after `AuthenticationMiddleware` if you want `request.user`. In Flask, register your own `before_request` hook first.

## Good to know before you deploy

- Rejected requests get 401 (unknown or revoked key), 403 (suspended customer), 429 (rate limit or quota), 402 (prepaid credit used up) or 503 (UsageRail unreachable and you chose to refuse). The body is `{"error": "...", "retry_after": 30}`. Pass `on_reject` to send your own response instead.
- Counts are sent every 60 seconds and once more when the app shuts down. Under uvicorn, FastAPI's lifespan shutdown sends them. Flask and Django send them when the process exits normally, which gunicorn does on SIGTERM; uvicorn re-raises SIGTERM after shutting down, so Django under uvicorn loses the counts it hadn't sent. If a process is killed hard (`kill -9`, out of memory), up to about 10 minutes of unsent counts are lost and never billed.
- With `gunicorn --preload`, each worker becomes a separate SDK instance after the fork and keeps the key list it inherited.
- The key list is cached in a private folder in the system temp directory, so a restart doesn't wait for the first download. Pass `snapshot=False` to turn this off.
- WebSocket connections are not checked or counted.
- Memory: around 15 MB with 10,000 keys and customers. At the limits (100,000 keys, 100,000 customers' settings, and the 40,000 customers used most recently) our benchmark measures about 81 MB on Python 3.12 and 87 MB on Python 3.10. Customers beyond that are fetched again when they come back, which adds one round trip to their next request.
- Other frameworks can use `usagerail.create_engine()` directly: call `begin()` when a request arrives and `end()` when it finishes.
