import { test } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, readdirSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import type { SyncSnapshot } from "../src/key-store.ts";
import { fileSnapshotStore } from "../src/node.ts";

const snapshot: SyncSnapshot = { saved_at: "2026-10-20T12:00:00.000Z", cursor: "c", app: null, keys: [], consumers: [] };
const unix = typeof process.getuid === "function";

function privateDirectory() {
  // mkdtemp creates it as 0700
  return path.join(mkdtempSync(path.join(tmpdir(), "ur-snapshot-test-")), "store");
}

test("a saved snapshot loads back; the directory is 0700 and the file 0600, with no temp files left", async () => {
  const directory = privateDirectory();
  const store = fileSnapshotStore("urk_test_one", directory);
  assert.equal(store.load(), null);
  await store.save(snapshot);
  assert.deepEqual(store.load(), snapshot);
  const files = readdirSync(directory);
  assert.equal(files.length, 1);
  if (unix) {
    assert.equal(statSync(directory).mode & 0o777, 0o700);
    assert.equal(statSync(path.join(directory, files[0])).mode & 0o777, 0o600);
  }
  // Different appKeys get different files
  assert.equal(fileSnapshotStore("urk_test_two", directory).load(), null);
});

test("a symlink or a file others can read in place of the snapshot is refused", { skip: !unix }, async () => {
  const directory = privateDirectory();
  const store = fileSnapshotStore("urk_test_links", directory);
  await store.save(snapshot);
  const [name] = readdirSync(directory);
  const file = path.join(directory, name);

  chmodSync(file, 0o644);
  assert.throws(() => store.load(), /not a private file/);

  const elsewhere = path.join(directory, "planted.json");
  writeFileSync(elsewhere, JSON.stringify(snapshot), { mode: 0o600 });
  const { rmSync } = await import("node:fs");
  rmSync(file);
  symlinkSync(elsewhere, file);
  assert.throws(() => store.load());
});

test("a snapshot directory that others can write to is refused for both reading and writing", { skip: !unix }, async () => {
  const directory = privateDirectory();
  const store = fileSnapshotStore("urk_test_dir", directory);
  await store.save(snapshot);
  chmodSync(directory, 0o777);
  assert.throws(() => store.load(), /only this user can access/);
  await assert.rejects(store.save(snapshot), /only this user can access/);
});

test("on SIGTERM, an app with its own shutdown handler keeps reporting (drain) instead of being closed early", async () => {
  const { flushOnExit } = await import("../src/node.ts");
  const calls: string[] = [];
  const appShutdown = () => calls.push("app");
  process.on("SIGTERM", appShutdown);
  const cancel = flushOnExit({ drain: () => calls.push("drain"), close: async () => void calls.push("close") });
  try {
    process.emit("SIGTERM");
    assert.deepEqual(calls.sort(), ["app", "drain"]);
  } finally {
    cancel();
    process.removeListener("SIGTERM", appShutdown);
  }
});

test("an app's once('SIGTERM') handler registered before the SDK still owns shutdown: no early close, no kill", async () => {
  const { flushOnExit } = await import("../src/node.ts");
  const calls: string[] = [];
  const kill = process.kill;
  process.kill = ((pid: number, signal?: string) => {
    calls.push(`kill ${signal}`);
    return true;
  }) as typeof process.kill;
  process.once("SIGTERM", () => calls.push("app"));
  const cancel = flushOnExit({ drain: () => calls.push("drain"), close: async () => void calls.push("close") });
  try {
    process.emit("SIGTERM");
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(calls.sort(), ["app", "drain"]);
  } finally {
    cancel();
    process.kill = kill;
  }
});

test("with no SIGTERM handler of the app's own, the SDK reports and then exits as SIGTERM would", async () => {
  const { flushOnExit } = await import("../src/node.ts");
  const calls: string[] = [];
  const kill = process.kill;
  process.kill = ((pid: number, signal?: string) => {
    calls.push(`kill ${signal}`);
    return true;
  }) as typeof process.kill;
  const cancel = flushOnExit({ drain: () => calls.push("drain"), close: async () => void calls.push("close") });
  try {
    process.emit("SIGTERM");
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(calls, ["close", "kill SIGTERM"]);
  } finally {
    cancel();
    process.kill = kill;
  }
});
