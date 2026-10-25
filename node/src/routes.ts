// Route weights and per-request reservations.
// Billing only trusts the template the framework reports: at the end of a request the weight is looked up by exact "method + template", and anything the seller didn't configure counts as 1.
// When a request starts, Express and Hono haven't matched a route yet, so there's no template, but the reservation has to be decided then: match the path against the seller's templates.
// That guess only sets the reservation, never what gets billed
import type { RouteSetting } from "./protocol.ts";

type Pattern = { setting: RouteSetting; method: string; regex: RegExp; regexIgnoringCase: RegExp };

export class RouteTable {
  private readonly exact = new Map<string, RouteSetting>();
  // Templates stored again in lowercase: Express routes case-insensitively by default, and a sub-router's mount path comes through as the request wrote it (/V1/generate), so lookups have to ignore case
  private readonly exactIgnoringCase = new Map<string, RouteSetting>();
  // Templates without parameters, stored again without a trailing slash (as written and lowercase), for matching paths at the start. Not everything becomes a regex: a seller can configure up to 500 routes,
  // and every request has to be matched against them
  private readonly literal = new Map<string, RouteSetting[]>();
  private readonly literalIgnoringCase = new Map<string, RouteSetting[]>();
  private readonly patterns: Pattern[] = [];

  constructor(settings: RouteSetting[]) {
    for (const setting of settings) {
      const method = normalizeMethod(setting.method);
      this.exact.set(`${method} ${setting.route}`, setting);
      const lowered = `${method} ${setting.route.toLowerCase()}`;
      if (!this.exactIgnoringCase.has(lowered)) this.exactIgnoringCase.set(lowered, setting);
      if (/[:*]/.test(setting.route)) {
        const source = templateToRegexSource(setting.route);
        this.patterns.push({ setting, method, regex: new RegExp(source), regexIgnoringCase: new RegExp(source, "i") });
      } else {
        append(this.literal, `${method} ${withoutTrailingSlash(setting.route)}`, setting);
        append(this.literalIgnoringCase, `${method} ${withoutTrailingSlash(setting.route.toLowerCase())}`, setting);
      }
    }
  }

  // At the end of a request: the "method + route template" the framework reports. If the framework routes case-insensitively (Express's default), so does the lookup
  byTemplate(method: string, template: string, ignoreCase = false): RouteSetting | undefined {
    if (this.exact.size === 0) return undefined;
    const found = this.exact.get(`${method} ${template}`) ?? this.exact.get(`* ${template}`);
    if (found || !ignoreCase) return found;
    const lowered = template.toLowerCase();
    return this.exactIgnoringCase.get(`${method} ${lowered}`) ?? this.exactIgnoringCase.get(`* ${lowered}`);
  }

  // At the start of a request: every template the path (without query string) matches, only to decide the reservation. There can be several (/users/me and /users/:id);
  // which one the framework picks is only known afterwards, so the caller reserves for the largest: better to reserve too much and refund at the end than too little and let requests through over the limit.
  // Case follows the framework's routing: Express ignores case by default (ignoreCase), Hono doesn't. Matching more narrowly than the framework lets a client change the casing, reach the same handler
  // and reserve only 1; matching more broadly makes the cheaper of two routes that differ only in case reserve like the expensive one, wrongly blocking customers close to their quota.
  // One trailing slash is always optional: being that much broader is fine, as hardly anyone prices two routes that differ only by a trailing slash differently
  candidatesByPath(method: string, path: string, ignoreCase = false): RouteSetting[] {
    const found: RouteSetting[] = [];
    const direct = this.byTemplate(method, path, ignoreCase);
    if (direct) found.push(direct);
    const literal = ignoreCase ? this.literalIgnoringCase : this.literal;
    const key = withoutTrailingSlash(ignoreCase ? path.toLowerCase() : path);
    for (const prefix of [method, "*"]) {
      for (const setting of literal.get(`${prefix} ${key}`) ?? []) if (setting !== direct) found.push(setting);
    }
    for (const pattern of this.patterns) {
      const regex = ignoreCase ? pattern.regexIgnoringCase : pattern.regex;
      if (pattern.setting !== direct && (pattern.method === method || pattern.method === "*") && regex.test(path)) found.push(pattern.setting);
    }
    return found;
  }
}

function append(map: Map<string, RouteSetting[]>, key: string, setting: RouteSetting) {
  map.set(key, [...(map.get(key) ?? []), setting]);
}

function normalizeMethod(method: string) {
  const upper = method.toUpperCase();
  return upper === "ALL" ? "*" : upper;
}

function withoutTrailingSlash(path: string) {
  return path.length > 1 && path.endsWith("/") ? path.slice(0, -1) : path;
}

// Supports the usual forms: /v1/users/:id, optional /:id?, Hono's constrained /:id{[0-9]+}, and a trailing wildcard *
function templateToRegexSource(template: string) {
  let source = "";
  const segments = template.split("/").slice(1);
  // A trailing slash on the template is treated as optional at the end, like one on the path. Empty segments in the middle (/v1//users) are part of the template
  // and kept: dropping them would miss /v1//users/7, which the framework does route here, and reserve only 1
  if (segments.length > 1 && segments.at(-1) === "") segments.pop();
  for (const segment of segments) {
    if (segment.startsWith("*")) {
      source += "(?:/.*)?";
    } else if (segment.startsWith(":")) {
      source += segment.endsWith("?") ? "(?:/[^/]+)?" : "/[^/]+";
    } else {
      source += `/${segment.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`;
    }
  }
  return `^${source}/?$`;
}
