// Route handlers for the Next.js App Router (Node runtime); also works for other serverless functions that take a Request and return a Response:
//
//   // lib/usagerail.ts: at module top level, so concurrent requests on one instance share it
//   import { after } from "next/server";
//   import { usageRail } from "@usagerail/node/next";
//   export const rail = usageRail({ appKey: process.env.USAGERAIL_APP_KEY!, auth: "api-key", waitUntil: after });
//
//   // app/v1/generate/route.ts
//   export const POST = rail.route("/v1/generate", async (request, { usage }) => {
//     const result = await generate(await request.json());
//     usage.track(result.tokens); // optional: meter custom units
//     return Response.json(result);
//   });
//
// On serverless, an instance may be frozen between requests and has no background timers: after each request, sync, lease renewal and reporting go to waitUntil
// (after from next/server in Next.js, waitUntil from @vercel/functions in Vercel functions). You write the route template yourself:
// the file path isn't available at runtime, and using the raw path would make every ID its own route.
import { createEngine, readApiKey, rejectionBody, type Engine, type EngineOptions, type Rejection, type RequestHandle } from "./engine.ts";
import { isEventStream, whenStreamEnds } from "./web.ts";

export type NextOptions = Omit<EngineOptions<Request>, "serverless" | "backgroundLoops" | "snapshotStore"> & {
  // What runs work that continues after the response: after from next/server in Next.js, waitUntil from @vercel/functions in Vercel functions
  waitUntil: (task: Promise<unknown>) => void;
  // Your own response for rejected requests. Without it, the response gets the matching status code and headers such as Retry-After, with body { error, retry_after? }
  onReject?: (rejection: Rejection, request: Request) => Response | Promise<Response>;
};

export type RouteHandler<Context> = (request: Request, context: Context & { usage: RequestHandle }) => Response | Promise<Response>;

export type UsageRail = {
  engine: Engine<Request>;
  // Wraps a route handler. template is the route template (/v1/items/[id]), the same one you configure weights for in the console
  route<Context extends object = object>(template: string, handler: RouteHandler<Context>): (request: Request, context: Context) => Promise<Response>;
};

// Next.js implements redirect() and permanentRedirect() by throwing (digest NEXT_REDIRECT;type;url;status;), and likewise
// notFound(), forbidden() and unauthorized() (NEXT_HTTP_ERROR_FALLBACK;status). Returns null for anything else
export function controlFlowStatus(error: unknown): number | null {
  const digest = typeof error === "object" && error !== null && "digest" in error ? (error as { digest: unknown }).digest : null;
  if (typeof digest !== "string") return null;
  const parts = digest.split(";");
  const status = parts[0] === "NEXT_REDIRECT" ? Number(parts.at(-2)) : parts[0] === "NEXT_HTTP_ERROR_FALLBACK" ? Number(parts[1]) : NaN;
  return Number.isInteger(status) && status >= 300 && status < 500 ? status : null;
}

// next build loads route modules, which runs usageRail(). If the build environment has no appKey (building in Docker and injecting secrets at runtime),
// skip creating the engine during the build; at runtime the check runs as usual and a missing key fails at startup. If the build actually calls the route (static prerendering), say why it can't run
const PRODUCTION_BUILD_PHASE = "phase-production-build";

function railWithoutKeyAtBuild(): UsageRail {
  const missing = () =>
    new Error("UsageRail: appKey is required (USAGERAIL_APP_KEY from the console). next build ran without it, so this route can't run at build time");
  return {
    get engine(): Engine<Request> {
      throw missing();
    },
    route() {
      return async () => {
        throw missing();
      };
    },
  };
}

export function usageRail(options: NextOptions): UsageRail {
  if (!options.appKey && globalThis.process?.env?.NEXT_PHASE === PRODUCTION_BUILD_PHASE) return railWithoutKeyAtBuild();
  const { waitUntil, onReject, ...engineOptions } = options;
  const engine = createEngine<Request>({ ...engineOptions, serverless: true });
  // The platform's waitUntil threw (e.g. after() called outside a request context): log a warning; it must not turn a successful response into an error
  const schedule = (task: Promise<unknown>) => {
    try {
      waitUntil(task);
    } catch (error) {
      (options.logger ?? console).warn(`UsageRail: waitUntil failed (${error instanceof Error ? error.message : String(error)}); this request's usage goes out with the next one`);
    }
  };
  // After the response goes out, hand sync, lease renewal and reporting to the platform, so the last request always takes the final batch with it
  const report = () => schedule(engine.afterResponse());

  return {
    engine,
    route<Context extends object = object>(template: string, handler: RouteHandler<Context>) {
      return async (request: Request, context: Context) => {
        let decision = engine.begin({
          method: request.method,
          path: new URL(request.url).pathname,
          route: template,
          apiKey: readApiKey(request.headers.get("authorization"), request.headers.get("x-api-key")),
          context: request,
        });
        if (decision instanceof Promise) decision = await decision;
        // The client left while we waited for a decision (remote key check, first lease): skip the handler, settle as aborted and refund the reservation (same as Express)
        if (decision.allowed && request.signal.aborted) {
          engine.end(decision.handle, { outcome: "aborted", route: template });
          report();
          return new Response(null, { status: 499 });
        }
        if (!decision.allowed) {
          // Rejected requests are recorded too (sdk_401, sdk_429 and so on), so they also need to be reported
          report();
          if (onReject) return onReject(decision, request);
          return Response.json(rejectionBody(decision), { status: decision.status, headers: { ...decision.headers } });
        }
        const handle = decision.handle;
        // The client left while the handler was still running (request.signal aborted): settle as aborted, not billed.
        // end() only records the first call, so the status the handler returns later doesn't count
        const onAbort = () => engine.end(handle, { outcome: "aborted", route: template });
        request.signal.addEventListener("abort", onAbort, { once: true });
        let response: Response;
        try {
          response = await handler(request, { ...context, usage: handle });
        } catch (error) {
          request.signal.removeEventListener("abort", onAbort);
          // Next.js turns thrown errors into a 500. redirect(), notFound() and friends are thrown too, but they're normal responses:
          // record their real status (307 is billed, 404 isn't), then rethrow for Next.js to handle
          engine.end(handle, { status: controlFlowStatus(error) ?? 500, route: template });
          report();
          throw error;
        }
        // Disconnects after the response is handed over (partway through a streamed body) are handled by the stream below
        request.signal.removeEventListener("abort", onAbort);
        // HEAD: Next.js answers with the GET handler and never reads the body. Waiting for that stream to end would wait forever, so settle it as a normal response and close the stream
        if (request.method === "HEAD" && response.body) {
          void response.body.cancel().catch(() => {});
          engine.end(handle, { status: response.status, route: template });
          report();
          return new Response(null, response);
        }
        if (isEventStream(response) && response.body) {
          // SSE: the stream has only just started when the handler returns. The connection counts once right away; units are settled by track() when the stream ends (fully read or client disconnected).
          // Report once when the stream opens (for that one count: the connection may stay open for hours, and the server won't accept it after 24 hours), and again when it ends
          engine.streamStarted(handle, template);
          report();
          let ended!: () => void;
          const streamEnded = new Promise<void>((resolve) => (ended = resolve));
          const body = whenStreamEnds(response.body, () => {
            engine.end(handle, { status: response.status, route: template });
            ended();
          });
          schedule(streamEnded.then(() => engine.afterResponse()));
          return new Response(body, response);
        }
        engine.end(handle, { status: response.status, route: template });
        report();
        return response;
      };
    },
  };
}
