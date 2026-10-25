// Framework-agnostic entry point. For a framework without an adapter, call createEngine, get a decision from begin(), and call end() when the request finishes.
// Pass the route template to begin() or end(). Billing only uses the template you pass in; a request with neither is recorded as _unmatched and counted as 1 unit.
// Express, Hono, Fastify and Next.js have their own adapters under @usagerail/node.
export {
  createEngine,
  readApiKey,
  rejectionBody,
  MAX_TRACK_UNITS,
  type Admission,
  type BeginRequest,
  type ConsumerIdentity,
  type Decision,
  type EndResult,
  type Engine,
  type EngineOptions,
  type Rejection,
  type RejectionCode,
  type RequestHandle,
} from "./engine.ts";
export type { Clock } from "./clock.ts";
export type { SnapshotStore, SyncSnapshot } from "./key-store.ts";
export { createFetchTransport, type Transport } from "./transport.ts";
