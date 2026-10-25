import { test } from "node:test";
import assert from "node:assert/strict";

import { RouteTable } from "../src/routes.ts";

const setting = (method: string, route: string, weight = 1) => ({ method, route, weight, reserve: weight });

// The tests only care about the first match (the exact one comes first)
const first = (table: RouteTable, method: string, path: string, ignoreCase = false) => table.candidatesByPath(method, path, ignoreCase)[0];

test("at the start of a request, paths match the seller's route templates; at the end the template is looked up exactly", () => {
  const table = new RouteTable([
    setting("POST", "/v1/images", 10),
    setting("GET", "/v1/users/:id", 2),
    setting("GET", "/v1/files/:name?", 3),
    setting("ALL", "/v1/batch/*", 4),
    setting("GET", "/v1/orders/:id{[0-9]+}", 5),
  ]);
  assert.equal(first(table, "POST", "/v1/images")?.weight, 10);
  assert.equal(first(table, "GET", "/v1/images"), undefined);
  assert.equal(first(table, "GET", "/v1/users/42")?.weight, 2);
  assert.equal(first(table, "GET", "/v1/users/42/")?.weight, 2);
  assert.equal(first(table, "GET", "/v1/users/42/posts"), undefined);
  assert.equal(first(table, "GET", "/v1/files")?.weight, 3);
  assert.equal(first(table, "GET", "/v1/files/a.txt")?.weight, 3);
  // ALL matches every method
  assert.equal(first(table, "DELETE", "/v1/batch/7/items")?.weight, 4);
  assert.equal(first(table, "GET", "/v1/orders/77")?.weight, 5);
  // At the end of a request the framework gives the template, looked up exactly
  assert.equal(table.byTemplate("GET", "/v1/users/:id")?.weight, 2);
  assert.equal(table.byTemplate("PUT", "/v1/batch/*")?.weight, 4);
  assert.equal(table.byTemplate("GET", "/v1/users/:userId"), undefined);
  // Regex special characters in the path match literally
  assert.equal(first(new RouteTable([setting("GET", "/v1/a.b")]), "GET", "/v1/aXb"), undefined);
});

test("case follows the framework: Express ignores it, Hono doesn't; one trailing slash is optional either way", () => {
  // Express ignores case and treats a trailing slash as optional by default: POST /V1/IMAGES/ still reaches the /v1/images handler, and missing it here would reserve only 1
  const table = new RouteTable([setting("POST", "/v1/images", 10), setting("GET", "/v1/users/:id/", 2), setting("GET", "/", 3)]);
  assert.equal(first(table, "POST", "/V1/IMAGES", true)?.weight, 10);
  assert.equal(first(table, "POST", "/v1/images/", true)?.weight, 10);
  assert.equal(first(table, "POST", "/v1/images//", true), undefined);
  assert.equal(first(table, "GET", "/v1/users/42")?.weight, 2);
  assert.equal(first(table, "GET", "/V1/Users/42/", true)?.weight, 2);
  assert.equal(first(table, "GET", "/")?.weight, 3);
  assert.equal(first(table, "GET", "/v1"), undefined);
  // Hono is case-sensitive: two routes differing only in case are separate, and the cheaper one doesn't reserve like the expensive one
  const hono = new RouteTable([setting("POST", "/v1/generate", 10), setting("POST", "/v1/GENERATE", 1), setting("GET", "/v1/users/:id", 2)]);
  assert.deepEqual(hono.candidatesByPath("POST", "/v1/GENERATE").map((item) => item.weight), [1]);
  assert.deepEqual(hono.candidatesByPath("GET", "/V1/users/7"), []);
  // Templates the framework gives at the end: looked up as is by default; case-insensitively for Express
  assert.equal(table.byTemplate("POST", "/V1/IMAGES"), undefined);
  assert.equal(table.byTemplate("POST", "/V1/images", true)?.route, "/v1/images");
});

test("every seller template that matches a path is a candidate, exact first, then in the order the seller saved them", () => {
  const table = new RouteTable([setting("GET", "/v1/users/:id", 10), setting("GET", "/v1/users/me", 1), setting("ALL", "/v1/*", 3)]);
  assert.deepEqual(table.candidatesByPath("GET", "/v1/users/me").map((item) => item.weight), [1, 10, 3]);
  assert.deepEqual(table.candidatesByPath("GET", "/v1/users/42").map((item) => item.weight), [10, 3]);
});
