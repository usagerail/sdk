# @usagerail/node

The UsageRail SDK for Node.js. It checks API keys, applies rate limits and quotas, and counts usage per customer inside your own API server, then reports the counts to UsageRail in the background. Requests never pass through us.

It needs Node.js 22 or newer and has no dependencies of its own. Versions start at 0.x, so names may still change between minor versions.

## Install

```bash
npm install @usagerail/node
```

## Express

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

Get the app key from the console at https://app.usagerail.com. The key a caller sends comes from the `Authorization: Bearer` header, or from `x-api-key` when there's no bearer token.

Hono and Fastify have their own entry points, `@usagerail/node/hono` and `@usagerail/node/fastify`.

## Docs

Setup for each framework, options, rate limits, quotas and known limitations: https://usagerail.com/docs
