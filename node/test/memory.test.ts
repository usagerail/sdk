import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// Keeps memory from growing back (target: under 50 MB at a typical scale, under 100 MB fully loaded). bench/memory.ts measures 20,000 keys plus 20,000 customers,
// averaged per entry: synced keys and consumer records take 254 bytes each (Node 26) to 288 bytes (Node 22); without dropping external IDs and sharing rate limit configurations
// it was 320 to 349 bytes. Once every customer is active, each adds under a kilobyte more
test("synced keys and customer records stay compact, and each active customer stays around a kilobyte", () => {
  const customers = 20_000;
  // Measuring needs gc first, and the test process wasn't started with --expose-gc, so run a separate one
  const output = execFileSync(process.execPath, ["--expose-gc", "bench/memory.ts", String(customers)], { cwd: fileURLToPath(new URL("..", import.meta.url)), encoding: "utf8" });
  const bytes = (label: string) => Number(new RegExp(`${label}: ([\\d.]+) MiB`).exec(output)?.[1]) * 2 ** 20;
  const synced = bytes("synced");
  const active = bytes("every customer has made a request");
  const perRecord = synced / (2 * customers);
  const perActive = (active - synced) / customers;
  assert.ok(perRecord < 305, `${perRecord.toFixed(0)} bytes per synced key or customer record`);
  assert.ok(perActive < 1200, `${perActive.toFixed(0)} bytes per active customer`);
});
