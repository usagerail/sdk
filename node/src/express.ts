// Express middleware. Add it before your routes:
//   app.use(usageRail({ appKey: process.env.USAGERAIL_APP_KEY, auth: "api-key" }));
// With your own auth, add it after your auth: auth: "custom", consumer: (req) => ({ id: req.user.id }).
// Metering custom units in a handler: (req as UsageRailRequest).usageRail.track(tokens)
import express, { type NextFunction, type Request, type Response } from "express";

import { createEngine, readApiKey, rejectionBody, type Decision, type Engine, type EngineOptions, type Rejection, type RequestHandle } from "./engine.ts";
import { UNMATCHED_ROUTE } from "./meter.ts";
import { fileSnapshotStore, flushOnExit, meterEventStreamsAtStart } from "./node.ts";

export type ExpressOptions = Omit<EngineOptions<Request>, "snapshotStore"> & {
  // Your own response for rejected requests. Without it, the response gets the matching status code and headers such as Retry-After, with body { error, retry_after? }
  onReject?: (rejection: Rejection, req: Request, res: Response) => void;
  // Save the key snapshot to the local temp directory and read it first on restart. On by default
  snapshot?: boolean;
  // Report remaining usage before exit (SIGTERM, beforeExit). On by default
  flushOnExit?: boolean;
};

export type UsageRailRequest = Request & { usageRail: RequestHandle };

// Lets TypeScript accept req.usageRail.track(units) directly. A type declaration only, erased at runtime
declare global {
  namespace Express {
    interface Request {
      usageRail?: RequestHandle;
    }
  }
}

export type UsageRailMiddleware = ((req: Request, res: Response, next: NextFunction) => void) & { engine: Engine<Request> };

export function usageRail(options: ExpressOptions): UsageRailMiddleware {
  const { onReject, snapshot, flushOnExit: hookExit, ...engineOptions } = options;
  const engine = createEngine<Request>({
    ...engineOptions,
    snapshotStore: snapshot === false || options.auth !== "api-key" ? undefined : fileSnapshotStore(options.appKey),
  });
  if (hookExit !== false) flushOnExit(engine);

  const respond = (decision: Decision, req: Request, res: Response, next: NextFunction, disconnected: boolean) => {
    if (!decision.allowed) {
      if (onReject) onReject(decision, req, res);
      else res.status(decision.status).set(decision.headers).json(rejectionBody(decision));
      return;
    }
    const handle = decision.handle;
    (req as UsageRailRequest).usageRail = handle;
    // The app this middleware belongs to: if the request ends up in another app (a sub-app), the template comes from the actual path (see declaredTemplate)
    const host = req.app;
    const routeTemplate = () => templateOf(req, host);
    meterEventStreamsAtStart(res, () => engine.streamStarted(handle, routeTemplate()));
    // A normal finish counts by status code; a client that disconnects before the response is sent counts as aborted. If the handler already sent headers and the client then disconnects, it counts by status code.
    // end() won't record again if the request was already recorded (when an SSE stream started)
    res.once("finish", () => engine.end(handle, { status: res.statusCode, route: routeTemplate() }));
    res.once("close", () => engine.end(handle, res.headersSent ? { status: res.statusCode, route: routeTemplate() } : { outcome: "aborted", route: routeTemplate() }));
    // The client disconnected while we waited for a decision (remote key check, first lease): nobody will receive a response, so skip the handler, settle as aborted and refund the reservation.
    // Carrying on as usual would let a client send and disconnect to make the handler work for free, neither billed nor counted
    if (disconnected) {
      engine.end(handle, { outcome: "aborted", route: routeTemplate() });
      return;
    }
    next();
  };

  const middleware = (req: Request, res: Response, next: NextFunction) => {
    const decision = engine.begin({
      method: req.method,
      // When mounted under a sub-path (app.use("/api", ...)), req.path lacks the mount path, but the seller configures full paths, so put it back
      path: `${req.baseUrl}${req.path}`,
      // Express routes case-insensitively by default; follow the app if it enables case sensitive routing. A sub-router's own caseSensitive isn't visible here, so the app's setting is used
      ignoreCase: !req.app.enabled("case sensitive routing"),
      apiKey: readApiKey(req.get("authorization"), req.get("x-api-key")),
      context: req,
    });
    if (decision instanceof Promise) {
      let disconnected = false;
      const onClose = () => (disconnected = true);
      res.once("close", onClose);
      const proceed = (resolved: Decision) => {
        res.removeListener("close", onClose);
        respond(resolved, req, res, next, disconnected);
      };
      // begin() doesn't throw; if it ever does, let the request through. The SDK must never leave a request hanging
      decision.then(proceed, () => {
        res.removeListener("close", onClose);
        next();
      });
    } else {
      respond(decision, req, res, next, false);
    }
  };
  return Object.assign(middleware, { engine });
}

// Route template: the sub-router's mount path + the route's own path, e.g. /v1 + /users/:id. Unmatched requests are recorded as _unmatched
function templateOf(req: Request, host: unknown) {
  const route = (req as Request & { route?: { path?: unknown } }).route;
  if (!route || route.path === undefined) return UNMATCHED_ROUTE;
  return (typeof route.path === "string" ? declaredTemplate(req, route, host) : null) ?? `${req.baseUrl}${String(route.path)}`;
}

// ---- Mount paths as declared ----
// Express's routing layers keep only the compiled matcher, not the path as written, and req.baseUrl at runtime holds the request's actual values (/tenants/42, /V1).
// Used as a template, a mount path with parameters or different casing wouldn't match the seller's template and would count as 1 unit (billing only trusts templates the framework reports).
// So importing this module wraps Router#use to record the declared path on each layer it creates; when settling, walk the app's routing table down to the route that handled
// the request and join the mount paths along the way (the OpenTelemetry Express instrumentation gets templates the same way).
// Import this module before the modules that define routes: anything mounted earlier has no recorded path and falls back to req.baseUrl
const MOUNT_PATH = Symbol.for("usagerail.express.mountPath");
const RECORDING = Symbol.for("usagerail.express.recordingMountPaths");

type Layer = { route?: unknown; handle?: unknown; [MOUNT_PATH]?: string };
type Stack = Layer[];
type App = { parent?: App; _router?: { stack?: Stack }; router?: { stack?: Stack } };

recordMountPaths();

function recordMountPaths() {
  // In Express 5, Router is a constructor with its methods on the prototype; in Express 4, Router itself is the prototype of every router
  const Router = express.Router as unknown as { prototype?: { use?: unknown }; use?: unknown };
  const proto = (typeof Router.prototype?.use === "function" ? Router.prototype : Router) as { use?: (...args: unknown[]) => unknown; [RECORDING]?: true };
  const use = proto.use;
  if (typeof use !== "function" || proto[RECORDING]) return;
  proto[RECORDING] = true;
  proto.use = function (this: { stack?: Stack }, ...args: unknown[]) {
    const before = this.stack?.length ?? 0;
    const result = use.apply(this, args);
    const path = declaredPath(args[0]);
    if (path !== undefined) for (const layer of this.stack?.slice(before) ?? []) layer[MOUNT_PATH] = path;
    return result;
  };
}

// Same rule Router#use uses to find the path: if the first argument (or the innermost first element of an array) isn't a function, it is the path; otherwise the path is "/".
// Path arrays and regexes aren't recorded; those mounts fall back to req.baseUrl
function declaredPath(first: unknown) {
  let innermost = first;
  while (Array.isArray(innermost) && innermost.length > 0) innermost = innermost[0];
  if (typeof innermost === "function") return "/";
  return typeof first === "string" ? first : undefined;
}

// Each route is looked up once per app, since routes don't change after registration. Kept per app, because one sub-router can be mounted in several apps
const templates = new WeakMap<object, WeakMap<object, string | null>>();

function declaredTemplate(req: Request, route: { path?: unknown }, host: unknown) {
  // Requests that go into a sub-app (created with express(), via app.use("/admin", adminApp) or router.use("/a", subApp)) use the actual path:
  // Express doesn't fully record where or how many times a sub-app is mounted (two mounts keep only the last, and under a router not even the level above), so a joined prefix might not be
  // the one this request took, and a wrong guess bills at another route's price. The cost: a sub-app mounted under a parameterized path is billed by concrete path, and unconfigured paths count as 1
  const app = req.app as unknown as App;
  if (app !== host || app.parent) return null;
  const forApp = templates.get(app) ?? new WeakMap<object, string | null>();
  templates.set(app, forApp);
  let template = forApp.get(route);
  if (template === undefined) {
    const stack = routerStack(app);
    template = (stack ? findRoute(stack, route, "") : null) ?? null;
    forApp.set(route, template);
  }
  return template;
}

function routerStack(app: App) {
  // Express 4 uses app._router (reading app.router throws); Express 5 uses app.router
  if (app._router) return app._router.stack;
  try {
    return app.router?.stack;
  } catch {
    return undefined;
  }
}

// Walk down to the route that handled this request, joining mount paths along the way. If the route is reachable along several paths (the same sub-router mounted twice),
// or a mount on the way has no recorded path, we can't tell which, so return null (fall back to req.baseUrl); return undefined if it isn't found
function findRoute(stack: Stack, route: object, prefix: string): string | null | undefined {
  let found: string | null | undefined;
  for (const layer of stack) {
    let here: string | null | undefined;
    if (layer.route === route) {
      here = `${prefix}${String((route as { path: unknown }).path)}`;
    } else if (layer.route === undefined && Array.isArray((layer.handle as { stack?: unknown } | undefined)?.stack)) {
      const mount = layer[MOUNT_PATH];
      const inner = findRoute((layer.handle as { stack: Stack }).stack, route, mount === undefined ? prefix : joinMount(prefix, mount));
      here = inner !== undefined && mount === undefined ? null : inner;
    }
    if (here === undefined) continue;
    if (found !== undefined && found !== here) return null;
    found = here;
  }
  return found;
}

// Mounting on "/" adds no prefix; Express doesn't include a mount path's trailing slash in baseUrl
function joinMount(prefix: string, mount: string) {
  if (mount === "/" || mount === "") return prefix;
  return `${prefix}${mount.endsWith("/") ? mount.slice(0, -1) : mount}`;
}
