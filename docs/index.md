# UsageRail SDKs

These are the open-source SDKs for UsageRail, an [API monetization platform](https://usagerail.com) for small teams that sell an API. Add one to your API server and it checks your customers' API keys, applies per-customer rate limits and quotas, and counts usage, all inside your own process. The SDK reports usage totals in the background. You see them in the UsageRail console and bill customers through your own Stripe account. UsageRail doesn't proxy your API requests.

Both SDKs are MIT licensed and have no runtime dependencies of their own.

| SDK | Install | Needs | Frameworks |
|---|---|---|---|
| [Node.js](node.md) | `npm install @usagerail/node` | Node.js 22+ | Express, Hono, Fastify, Next.js |
| [Python](python.md) | `pip install usagerail` | Python 3.10+ | FastAPI, Starlette, Flask, Django |

## What happens inside your process

- Key checks run against a synced copy of your app's key hashes, kept in memory. A key that hasn't reached this instance yet is checked with UsageRail, with a 300 ms timeout.
- Right after startup, with no saved key snapshot, requests wait for the first sync, up to 1 second.
- Per-second and per-minute rate limits are checked in memory. By default, a request over a rate limit gets a 429 with a `Retry-After` header.
- On stop-at-limit and prepaid quotas, a request can wait while the SDK reserves more of the customer's quota from UsageRail.
- The SDK groups usage into one-minute totals in memory. On a long-running server it sends a batch every 60 seconds, or sooner if the batch fills up. In Next.js, reporting runs after responses.
- Request and response bodies aren't sent to UsageRail, and the SDK doesn't record IP addresses or raw API keys.

## Timings to plan around

- When UsageRail is reachable, running SDK instances usually pick up new keys and revocations within about 20 seconds. During an outage, an instance can keep accepting a revoked key from its cached list.
- Rate limits are counted per server instance by default.
- If a process is killed (`kill -9` or out of memory), unsent usage is lost and won't be billed. The SDK keeps queued batches in memory for up to about 10 minutes. In Next.js, a retry waits for another request to finish.

## Pages here

- [Node.js](node.md): install and the Express example. Hono and Fastify have their own entry points.
- [Python](python.md): FastAPI, Flask and Django examples, your own authentication, and what to check before you deploy.
- [Coding agent skill](agent-skill.md): give this to your coding agent when it adds the SDK to an existing API. It shows how to keep health checks, webhook receivers and CORS preflight working without a UsageRail key.

Framework guides, configuration options and the HTTP check endpoint for other languages are in the [UsageRail docs](https://usagerail.com/docs).

## Source, issues and releases

The source is at [github.com/usagerail/sdk](https://github.com/usagerail/sdk){rel=nofollow}, where issues and pull requests are welcome. Every version on npm and PyPI from 0.1.1 on is built by that repository's release workflow from a tag.
