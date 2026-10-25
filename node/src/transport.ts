// The four endpoints between the SDK and the server. The engine depends only on this interface,
// so a server-side implementation can read the database directly instead of going over HTTP.
import type {
  IngestRequest,
  IngestResponse,
  LeaseRequest,
  LeaseResponse,
  SyncResponse,
  VerifyKeyRequest,
  VerifyKeyResponse,
} from "./protocol.ts";

export const DEFAULT_BASE_URL = "https://api.usagerail.com";

// Key verification and lease requests hold up the caller's request, so they are capped at 300 ms. Sync and ingest run in the background and get plenty of time
export const DEFAULT_TIMEOUTS = { verifyKey: 300, leases: 300, sync: 10_000, ingest: 10_000 };

export type CallOptions = { timeoutMs?: number };

export type Transport = {
  ingest(request: IngestRequest, options?: CallOptions): Promise<IngestResponse>;
  sync(cursor: string | null, options?: CallOptions): Promise<SyncResponse>;
  verifyKey(request: VerifyKeyRequest, options?: CallOptions): Promise<VerifyKeyResponse>;
  leases(request: LeaseRequest, options?: CallOptions): Promise<LeaseResponse>;
};

// Thrown for non-2xx responses, timeouts, network errors and unreadable bodies. status is null when there was no usable response; treated as a temporary failure
export class TransportError extends Error {
  readonly status: number | null;
  readonly retryAfterMs: number | null;

  constructor(message: string, status: number | null, retryAfterMs: number | null = null) {
    super(message);
    this.name = "TransportError";
    this.status = status;
    this.retryAfterMs = retryAfterMs;
  }
}

export type FetchTransportOptions = {
  appKey: string;
  instanceId: string;
  baseUrl?: string;
  fetch?: typeof fetch;
  timeouts?: Partial<typeof DEFAULT_TIMEOUTS>;
  // Serverless instances commit after every request; this header tells the server to allow more ingest batches (120 a minute)
  instanceKind?: "serverless";
};

export function createFetchTransport(options: FetchTransportOptions): Transport {
  const baseUrl = (options.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const timeouts = { ...DEFAULT_TIMEOUTS, ...options.timeouts };
  const headers: Record<string, string> = {
    authorization: `Bearer ${options.appKey}`,
    "x-instance-id": options.instanceId,
    ...(options.instanceKind ? { "x-instance-kind": options.instanceKind } : {}),
  };

  async function call<T>(method: "GET" | "POST", path: string, body: unknown, timeoutMs: number): Promise<T> {
    let response: Response;
    try {
      response = await fetchImpl(baseUrl + path, {
        method,
        headers: body === undefined ? headers : { ...headers, "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
        // Timeouts are often the remaining time from the monotonic clock, with a fractional part; AbortSignal.timeout only takes integers
        signal: AbortSignal.timeout(Math.max(1, Math.ceil(timeoutMs))),
      });
    } catch (error) {
      throw new TransportError(`${method} ${path} failed: ${describeError(error)}`, null);
    }
    if (!response.ok) {
      // Errors are judged by status code only. Read and discard the body so the connection can go back to the pool
      await response.body?.cancel().catch(() => {});
      const retryAfterMs = parseRetryAfter(response.headers.get("retry-after"));
      throw new TransportError(`${method} ${path} returned ${response.status}`, response.status, retryAfterMs);
    }
    try {
      return (await response.json()) as T;
    } catch (error) {
      throw new TransportError(`${method} ${path} returned an unreadable body: ${describeError(error)}`, null);
    }
  }

  return {
    ingest: (request, callOptions) => call("POST", "/v1/ingest", request, callOptions?.timeoutMs ?? timeouts.ingest),
    sync: (cursor, callOptions) =>
      call("GET", cursor === null ? "/v1/sync" : `/v1/sync?cursor=${encodeURIComponent(cursor)}`, undefined, callOptions?.timeoutMs ?? timeouts.sync),
    verifyKey: (request, callOptions) => call("POST", "/v1/keys/verify", request, callOptions?.timeoutMs ?? timeouts.verifyKey),
    leases: (request, callOptions) => call("POST", "/v1/leases", request, callOptions?.timeoutMs ?? timeouts.leases),
  };
}

// Configuration errors (revoked app key 401, deleted app 410, wrong integration mode 409, wrong URL 404, etc.): retrying won't help.
// Ingest and sync both pause and try again every 10 minutes. 400 (a problem with this batch), 413 (split the batch),
// 408 and 429 (temporary) don't count
export function isPausingStatus(status: number | null) {
  return status !== null && status >= 400 && status < 500 && status !== 400 && status !== 408 && status !== 413 && status !== 429;
}

// Retry-After can be a number of seconds or an HTTP date (RFC 9110)
export function parseRetryAfter(value: string | null, now = Date.now()): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - now) : null;
}

export function describeError(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
