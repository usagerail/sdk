// Fastify plugin. Register it before your routes:
//   await app.register(usageRail({ appKey: process.env.USAGERAIL_APP_KEY, auth: "api-key" }));
// With your own auth: auth: "custom", consumer: (request) => ({ id: request.user.id }); the hook has to run after your auth.
// Metering custom units in a handler: request.usageRail.track(tokens)
import type { Socket } from "node:net";

import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";

import { createEngine, readApiKey, rejectionBody, type Engine, type EngineOptions, type Rejection, type RequestHandle } from "./engine.ts";
import { UNMATCHED_ROUTE } from "./meter.ts";
import { fileSnapshotStore, flushOnExit, meterEventStreamsAtStart } from "./node.ts";

export type FastifyOptions = Omit<EngineOptions<FastifyRequest>, "snapshotStore"> & {
  // Your own response for rejected requests. Without it, the response gets the matching status code and headers such as Retry-After, with body { error, retry_after? }
  onReject?: (rejection: Rejection, request: FastifyRequest, reply: FastifyReply) => void | Promise<void>;
  // Save the key snapshot to the local temp directory and read it first on restart. On by default
  snapshot?: boolean;
  // Report remaining usage before exit (SIGTERM, beforeExit). On by default
  flushOnExit?: boolean;
};

// Lets TypeScript accept request.usageRail.track(units) directly. Rejected requests never reach a handler, so in a handler it is always set
declare module "fastify" {
  interface FastifyRequest {
    usageRail: RequestHandle;
  }
}

export type UsageRailPlugin = ((fastify: FastifyInstance) => Promise<void>) & { engine: Engine<FastifyRequest> };

export function usageRail(options: FastifyOptions): UsageRailPlugin {
  const { onReject, snapshot, flushOnExit: hookExit, ...engineOptions } = options;
  const engine = createEngine<FastifyRequest>({
    ...engineOptions,
    snapshotStore: snapshot === false || options.auth !== "api-key" ? undefined : fileSnapshotStore(options.appKey),
  });
  if (hookExit !== false) flushOnExit(engine);

  const plugin = async (fastify: FastifyInstance) => {
    fastify.decorateRequest("usageRail", null as unknown as RequestHandle);

    fastify.addHook("onRequest", async (request, reply) => {
      // The client disconnected while we waited for a decision (remote key check, first lease): note it, and don't pass the request to the handler once the decision arrives
      let disconnected = false;
      const onClose = () => (disconnected = true);
      reply.raw.once("close", onClose);
      let decision;
      try {
        const pending = engine.begin({
          method: request.method,
          // Fastify has already routed by now, so reserve by the template it matched and settle by it at the end too. The raw URL won't do: /v1/%67enerate reaches the /v1/generate handler
          // Unrouted requests are _unmatched: don't guess the seller's template from the URL, or a 404 could reserve like an expensive route and get a 429
          route: request.routeOptions?.url ?? UNMATCHED_ROUTE,
          path: request.url.split("?")[0],
          apiKey: readApiKey(request.headers.authorization, headerOf(request.headers["x-api-key"])),
          context: request,
        });
        decision = pending instanceof Promise ? await pending : pending;
      } catch {
        // begin() doesn't throw; if it ever does, let the request through. The SDK must never leave a request hanging
        return;
      } finally {
        reply.raw.removeListener("close", onClose);
      }
      const upgrade = String(request.headers.upgrade ?? "").toLowerCase() === "websocket";
      if (!decision.allowed) {
        // A blocked handshake: the connection @fastify/websocket hands over has already left the HTTP server, so close it ourselves after rejecting, or it stays open.
        // If @fastify/websocket is registered first it closes it too; if it's registered after, its hook never gets here
        if (upgrade) reply.raw.once("finish", () => request.raw.socket?.destroy());
        if (onReject) await onReject(decision, request, reply);
        else reply.code(decision.status).headers(decision.headers).send(rejectionBody(decision));
        return reply;
      }
      const handle = decision.handle;
      request.usageRail = handle;
      const route = () => routeOf(request);
      // If the client left while earlier hooks ran (your own async auth, say), the close event has already fired and the listener above missed it, so check the connection state
      if (disconnected || reply.raw.destroyed) {
        // Nobody to receive the response: settle as aborted, refund the reservation and skip the handler. Otherwise a client that sends and disconnects makes the handler work for free, neither billed nor counted
        engine.end(handle, { outcome: "aborted", route: route() });
        reply.hijack();
        return reply;
      }
      // WebSocket (@fastify/websocket): on a successful handshake ws writes 101 straight to the socket, bypassing reply, onResponse and the header checks.
      // Watch the first write on this socket: if it's a 101, a long-lived connection opened and counts once right away, settled by track() when it closes, like SSE
      // An Upgrade header doesn't mean the request was actually upgraded: plain SSE still goes through the header check, the 101 handshake is watched separately
      meterEventStreamsAtStart(reply.raw, () => engine.streamStarted(handle, route()));
      if (upgrade) meterWebSocketHandshake(request.raw.socket, () => engine.streamStarted(handle, route()));
      // A client that disconnects before the response is sent counts as aborted; once headers are out, a later disconnect counts by status code. end() only records the first call
      reply.raw.once("close", () => engine.end(handle, reply.raw.headersSent ? { status: reply.raw.statusCode, route: route() } : { outcome: "aborted", route: route() }));
    });

    fastify.addHook("onResponse", async (request, reply) => {
      if (request.usageRail) engine.end(request.usageRail, { status: reply.statusCode, route: routeOf(request) });
    });
  };
  // Skip Fastify's encapsulation: the hooks must apply to the whole app, not just routes registered inside this plugin (the same thing fastify-plugin does)
  Object.assign(plugin, { [Symbol.for("skip-override")]: true });
  return Object.assign(plugin, { engine });
}

// Route template: Fastify matches the route before onRequest, including the registration prefix, e.g. /v1/users/:id. Unmatched requests are recorded as _unmatched
function routeOf(request: FastifyRequest) {
  return request.routeOptions?.url ?? UNMATCHED_ROUTE;
}

// ws writes the handshake response straight to the socket: a first write starting with "HTTP/1.1 101" means it succeeded. Restore the original write after the first one
function meterWebSocketHandshake(socket: Socket, onOpen: () => void) {
  const write = socket.write;
  socket.write = function (this: Socket, chunk: unknown, ...rest: unknown[]) {
    socket.write = write;
    const head = typeof chunk === "string" ? chunk.slice(0, 12) : chunk instanceof Uint8Array ? Buffer.from(chunk.buffer, chunk.byteOffset, Math.min(12, chunk.byteLength)).toString("latin1") : "";
    if (head === "HTTP/1.1 101") onOpen();
    return (write as (...args: unknown[]) => boolean).apply(this, [chunk, ...rest]);
  } as Socket["write"];
}

function headerOf(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}
