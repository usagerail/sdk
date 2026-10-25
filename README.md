# UsageRail SDKs

Source for the UsageRail SDKs. They check API keys, apply rate limits and quotas, and count usage per customer inside your own API server, then report the counts to UsageRail in the background. Requests never pass through us.

| SDK | Install | Frameworks | Source |
|---|---|---|---|
| Node.js 22+ | `npm install @usagerail/node` | Express, Hono, Fastify, Next.js | [node/](node) |
| Python 3.10+ | `pip install usagerail` | FastAPI, Starlette, Flask, Django | [python/](python) |

Setup guides and the API reference are at [usagerail.com/docs](https://usagerail.com/docs).

## Issues and pull requests

Both are welcome here. The SDKs are developed in UsageRail's main codebase, next to the service they talk to, and this repository is published from there. An accepted pull request is applied upstream and comes back here with the next sync, credited to you.

## Releases

From 0.1.1 on, every version on npm and PyPI is built and published by the [release workflow](.github/workflows/release.yml) from a tag in this repository (`node-v0.1.1`, `python-v0.1.1`). npm shows the commit each of those versions was built from.

## License

MIT
