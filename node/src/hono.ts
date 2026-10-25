// Hono middleware:
//   app.use("*", usageRail({ appKey: process.env.USAGERAIL_APP_KEY, auth: "api-key" }));
// Metering custom units in a handler: c.get("usageRail").track(tokens)
// On serverless platforms such as Cloudflare Workers, add serverless: true: no background timers; after each request, sync, lease renewal and reporting are handed to
// c.executionCtx.waitUntil. On Workers, read appKey at module top level with import { env } from "cloudflare:workers".
// The engine needs node:crypto, so enable nodejs_compat. Workers don't allow generating random numbers at module top level (the engine generates an instance ID as soon as it's created),
// so in serverless mode the engine is created on the first request and reused after that
import type { Context, MiddlewareHandler } from "hono";
import { routePath } from "hono/route";

import { createEngine, readApiKey, rejectionBody, type EndResult, type Engine, type EngineOptions, type Rejection, type RequestHandle } from "./engine.ts";
import { isEventStream, whenStreamEnds } from "./web.ts";

export type HonoOptions = EngineOptions<Context> & {
  // Your own response for rejected requests. Without it, the response gets the matching status code and headers such as Retry-After, with body { error, retry_after? }
  onReject?: (rejection: Rejection, c: Context) => Response | Promise<Response>;
};

export type UsageRailVariables = { usageRail: RequestHandle };

export type UsageRailMiddleware = MiddlewareHandler<{ Variables: UsageRailVariables }> & { engine: Engine<Context> };

export function usageRail(options: HonoOptions): UsageRailMiddleware {
  const { onReject, ...engineOptions } = options;
  let created: Engine<Context> | null = options.serverless ? null : createEngine<Context>(engineOptions);
  const engineOf = () => (created ??= createEngine<Context>(engineOptions));

  const middleware: MiddlewareHandler<{ Variables: UsageRailVariables }> = async (c, next) => {
    const engine = engineOf();
    const ownIndex = c.req.routeIndex;
    // Don't guess at the start which route will handle the request: a wrong guess (an app.all or a catch-all mounted later, say) reserves too little for a cheap route.
    // Match the raw path against the seller's templates instead, and reserve for the largest if several match (engine.ts)
    let decision = engine.begin({
      method: c.req.method,
      path: c.req.path,
      apiKey: readApiKey(c.req.header("authorization"), c.req.header("x-api-key")),
      context: c,
    });
    if (decision instanceof Promise) decision = await decision;
    if (!decision.allowed) {
      // Rejected requests are recorded too (sdk_401, sdk_429 and so on), so on serverless they also need to be reported
      report(c);
      if (onReject) return onReject(decision, c);
      return c.json(rejectionBody(decision), decision.status, { ...decision.headers });
    }
    const handle = decision.handle;
    c.set("usageRail", handle);
    try {
      await next();
    } catch (error) {
      // Hono usually passes handler errors to onError, so they rarely reach here; anything that does is recorded as 5xx and rethrown
      engine.end(handle, { status: 500, route: executedTemplate(c, ownIndex) });
      report(c);
      throw error;
    }
    const route = executedTemplate(c, ownIndex);
    const body = c.res.body;
    // HEAD: Hono answers with the GET handler and then discards the body without reading it. Waiting for that stream to end would wait forever, so settle it as a normal response and close the stream
    if (c.req.method === "HEAD" && body) {
      void body.cancel().catch(() => {});
      engine.end(handle, endResult(c, route));
      report(c);
      return;
    }
    if (isEventStream(c.res) && body) {
      // SSE: the stream has only just started when the handler returns. The connection counts once right away; units are settled by track() when the stream ends (fully read or client disconnected).
      // On serverless, report once when the stream opens (for that one count: the connection may stay open for hours, and the server won't accept it after 24 hours),
      // and again when it ends (for the units)
      engine.streamStarted(handle, route);
      report(c);
      let ended!: () => void;
      const streamEnded = new Promise<void>((resolve) => (ended = resolve));
      c.res = new Response(
        whenStreamEnds(body, () => {
          engine.end(handle, { status: c.res.status, route });
          ended();
        }),
        c.res,
      );
      report(c, streamEnded);
      return;
    }
    engine.end(handle, endResult(c, route));
    report(c);
  };

  // Serverless: hand sync, lease renewal and reporting to the platform's waitUntil after the response
  const report = (c: Context, after: Promise<void> = Promise.resolve()) => {
    if (!options.serverless) return;
    let waitUntil: ((task: Promise<unknown>) => void) | undefined;
    try {
      waitUntil = c.executionCtx.waitUntil.bind(c.executionCtx);
    } catch {
      // Not on a platform with an ExecutionContext like Workers: there's no way to keep running after the response, so report here (without awaiting)
    }
    const task = after.then(() => engineOf().afterResponse());
    try {
      waitUntil?.(task);
    } catch (error) {
      // waitUntil threw: log a warning. It must not turn a successful response into an error, or hide the handler's own exception
      (options.logger ?? console).warn(`UsageRail: waitUntil failed (${error instanceof Error ? error.message : String(error)}); this request's usage goes out with the next one`);
    }
  };
  return Object.defineProperty(middleware, "engine", { get: engineOf, enumerable: true }) as UsageRailMiddleware;
}

// At the end of a request: the route that actually handled it (routeIndex stops at whichever handler Hono ran). If it stopped at this middleware, no route handled it: record _unmatched
function executedTemplate(c: Context, ownIndex: number) {
  return c.req.routeIndex === ownIndex ? undefined : routePath(c);
}

function endResult(c: Context, route: string | undefined): EndResult {
  // A successful WebSocket handshake opens one long-lived connection and counts once
  if (c.res.status === 101) return { outcome: "ws_open", route };
  return { status: c.res.status, route };
}
