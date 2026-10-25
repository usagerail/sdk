import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";

import { checksumFor, generateApiKey } from "../src/key-format.ts";
import { FRESH_FOR_MS, KeyStore, NEGATIVE_CACHE_MS, REMOTE_VERIFIES_PER_SECOND, SNAPSHOT_MAX_AGE_MS, VERIFY_OUTAGE_MS } from "../src/key-store.ts";
import type { VerifyKeyResponse } from "../src/protocol.ts";
import { consumerRecord, FakeClock, FakeTransport, keyRecord } from "./helpers.ts";

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

function setup() {
  const clock = new FakeClock();
  const transport = new FakeTransport();
  const store = new KeyStore({ clock, transport, warn: () => {} });
  return { clock, transport, store };
}

function newKey() {
  return generateApiKey({ prefix: "acme", environment: "test" });
}

function syncWith(store: KeyStore, records: ReturnType<typeof keyRecord>[]) {
  store.applySyncPage({ full: true, keys: records, removed_keys: [] });
  store.completeSync();
}

test("a key with a bad format or checksum is rejected locally, without a network call", () => {
  const { transport, store } = setup();
  const key = newKey().key;
  const tampered = key.slice(0, -1) + (key.endsWith("A") ? "B" : "A");
  for (const candidate of [undefined, null, "", "not-a-key", tampered]) {
    assert.deepEqual(store.check(candidate, "allow"), { result: "invalid", consumer: null });
  }
  assert.equal(transport.verifyCalls.length, 0);
});

test("snapshot keys are checked by hash, then status and local expiry (steps 2–4)", () => {
  const { clock, store } = setup();
  const good = newKey();
  const revoked = newKey();
  const expiring = newKey();
  syncWith(store, [
    keyRecord({ key_id: good.keyId, hash: sha256(good.key), consumer: "cons_good", quota_behavior: "stop", quota_version: 3 }),
    keyRecord({ key_id: revoked.keyId, hash: sha256(revoked.key), consumer: "cons_revoked", status: "revoked" }),
    keyRecord({ key_id: expiring.keyId, hash: sha256(expiring.key), consumer: "cons_expiring", expires_at: new Date(clock.now() + 60_000).toISOString() }),
  ]);
  assert.deepEqual(store.check(good.key, "allow"), {
    result: "valid",
    keyId: good.keyId,
    consumer: "cons_good",
    quotaBehavior: "stop",
    quotaVersion: 3,
    consumerRecord: null,
  });
  // Same key ID with a different secret and a correct checksum: passes the format check, but the hash doesn't match
  const body = `acme_test_${good.keyId}${newKey().key.slice(18, -6)}`;
  assert.deepEqual(store.check(body + checksumFor(body), "allow"), { result: "invalid", consumer: null });
  // Revoked: we know who it is, so sdk_401 is recorded against them
  assert.deepEqual(store.check(revoked.key, "allow"), { result: "invalid", consumer: "cons_revoked" });
  assert.equal((store.check(expiring.key, "allow") as { result: string }).result, "valid");
  clock.wall += 60_000;
  assert.deepEqual(store.check(expiring.key, "allow"), { result: "invalid", consumer: "cons_expiring" });
});

test("the freshness table decides what happens to keys missing from the snapshot", async () => {
  const { clock, transport, store } = setup();
  const known = newKey();
  const unknown = newKey();
  transport.onVerify = () => ({ valid: false, reason: "not_found" });
  // No snapshot: remote verification
  assert.equal(store.freshness(), "none");
  assert.ok(store.check(unknown.key, "deny") instanceof Promise);
  syncWith(store, [keyRecord({ key_id: known.keyId, hash: sha256(known.key) })]);
  assert.equal(store.freshness(), "fresh");

  // Stale (no successful sync for 10 minutes) + allow: keys in the snapshot are judged locally, others are verified remotely
  clock.mono += FRESH_FOR_MS + 1;
  assert.equal(store.freshness(), "stale");
  assert.equal((store.check(known.key, "allow") as { result: string }).result, "valid");
  const second = newKey();
  assert.ok(store.check(second.key, "allow") instanceof Promise);
  // Stale + deny: keys missing from the snapshot get a 401 straight away, no remote verification
  const calls = transport.verifyCalls.length;
  assert.deepEqual(store.check(newKey().key, "deny"), { result: "invalid", consumer: null });
  assert.equal(transport.verifyCalls.length, calls);
  assert.equal((store.check(known.key, "deny") as { result: string }).result, "valid");

  // Past 24 hours: the whole snapshot counts as none, and even keys in it are verified remotely
  clock.mono += SNAPSHOT_MAX_AGE_MS;
  assert.equal(store.freshness(), "none");
  assert.ok(store.check(known.key, "deny") instanceof Promise);
  await Promise.resolve();
});

test("remote verification: positive results last until the next sync, negative ones 60 seconds", async () => {
  const { clock, transport, store } = setup();
  syncWith(store, []);
  const fresh = newKey();
  const unknown = newKey();
  transport.onVerify = (request): VerifyKeyResponse =>
    request.key_id === fresh.keyId
      ? { valid: true, key: keyRecord({ key_id: fresh.keyId, hash: sha256(fresh.key), consumer: "cons_new" }), consumer: consumerRecord({ consumer: "cons_new" }) }
      : { valid: false, reason: "not_found" };

  const first = await store.check(fresh.key, "allow");
  assert.equal(first.result, "valid");
  assert.equal(first.result === "valid" && first.consumerRecord?.consumer, "cons_new");
  // No remote verification the second time
  assert.equal((store.check(fresh.key, "allow") as { result: string }).result, "valid");
  assert.equal(transport.verifyCalls.length, 1);
  // Dropped after the next sync (a new key should be in the snapshot by then; this sync doesn't have it, so it's verified again)
  syncWith(store, []);
  assert.ok(store.check(fresh.key, "allow") instanceof Promise);

  assert.equal((await store.check(unknown.key, "allow")).result, "invalid");
  const calls = transport.verifyCalls.length;
  assert.deepEqual(store.check(unknown.key, "allow"), { result: "invalid", consumer: null });
  clock.mono += NEGATIVE_CACHE_MS;
  assert.ok(store.check(unknown.key, "allow") instanceof Promise);
  assert.equal(transport.verifyCalls.length, calls + 1);
});

test("at most 20 remote verifications per second; concurrent checks of one key share a call", async () => {
  const { clock, transport, store } = setup();
  syncWith(store, []);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  transport.onVerify = async () => {
    await gate;
    return { valid: false, reason: "not_found" };
  };
  const shared = newKey();
  const a = store.check(shared.key, "allow");
  const b = store.check(shared.key, "allow");
  assert.equal(a, b);
  const results = [a];
  for (let index = 1; index < REMOTE_VERIFIES_PER_SECOND; index++) results.push(store.check(newKey().key, "allow"));
  assert.equal(transport.verifyCalls.length, REMOTE_VERIFIES_PER_SECOND);
  // The 21st: 401 straight away
  assert.deepEqual(store.check(newKey().key, "allow"), { result: "invalid", consumer: null });
  release();
  await Promise.all(results);
  clock.mono += 1_000;
  assert.ok(store.check(newKey().key, "allow") instanceof Promise);
});

test("before the first sync, checks over the 20-per-second budget are 'unavailable', not 401, because every key is unknown then", () => {
  const { transport, store } = setup();
  transport.onVerify = () => new Promise<VerifyKeyResponse>(() => {});
  for (let index = 0; index < REMOTE_VERIFIES_PER_SECOND; index++) assert.ok(store.check(newKey().key, "allow") instanceof Promise);
  assert.deepEqual(store.check(newKey().key, "allow"), { result: "unavailable" });
  // Once synced, keys missing from the snapshot beyond the budget still get a 401
  syncWith(store, []);
  assert.deepEqual(store.check(newKey().key, "allow"), { result: "invalid", consumer: null });
});

test("when remote verification fails, new keys are 'unavailable' for a few seconds without waiting again", async () => {
  const { clock, transport, store } = setup();
  syncWith(store, []);
  transport.onVerify = () => {
    throw new Error("connection refused");
  };
  assert.deepEqual(await store.check(newKey().key, "allow"), { result: "unavailable" });
  assert.deepEqual(store.check(newKey().key, "allow"), { result: "unavailable" });
  assert.equal(transport.verifyCalls.length, 1);
  clock.mono += VERIFY_OUTAGE_MS;
  assert.ok(store.check(newKey().key, "allow") instanceof Promise);
});

test("a paged full sync removes keys that are no longer listed only after the last page", () => {
  const { store } = setup();
  const a = newKey();
  const b = newKey();
  syncWith(store, [keyRecord({ key_id: a.keyId, hash: sha256(a.key) }), keyRecord({ key_id: b.keyId, hash: sha256(b.key) })]);
  // A new full sync: the first page has only a, the second has nothing
  store.applySyncPage({ full: true, keys: [keyRecord({ key_id: a.keyId, hash: sha256(a.key) })], removed_keys: [] });
  assert.equal((store.check(b.key, "allow") as { result: string }).result, "valid");
  store.applySyncPage({ full: true, keys: [], removed_keys: [] });
  store.completeSync();
  assert.ok(store.check(b.key, "allow") instanceof Promise);
  // a from the first page is still there
  assert.equal((store.check(a.key, "allow") as { result: string }).result, "valid");
  // A key removed in an incremental sync is gone at once
  store.applySyncPage({ full: false, keys: [], removed_keys: [a.keyId] });
  assert.ok(store.check(a.key, "allow") instanceof Promise);
});

test("a forged secret for a known key ID doesn't get the real key refused (key IDs aren't secret)", async () => {
  const { transport, store } = setup();
  syncWith(store, []);
  const real = newKey();
  transport.onVerify = (request): VerifyKeyResponse =>
    request.hash === sha256(real.key)
      ? { valid: true, key: keyRecord({ key_id: real.keyId, hash: sha256(real.key), consumer: "cons_new" }), consumer: consumerRecord({ consumer: "cons_new" }) }
      : { valid: false, reason: "not_found" };
  // Same key ID, made-up secret, correct checksum
  const body = `acme_test_${real.keyId}${newKey().key.slice(18, -6)}`;
  assert.equal((await store.check(body + checksumFor(body), "allow")).result, "invalid");
  assert.equal((await store.check(real.key, "allow")).result, "valid");
});

// ---- Found while reviewing the Python SDK, same in Node: a late remote verification doesn't override a newer sync ----
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => (resolve = done));
  return { promise, resolve };
}

test("a verification that comes back after a newer sync is not remembered", async () => {
  // A sync completed while the verification was in flight (the key may have been revoked or deleted meanwhile): the late "valid" isn't remembered,
  // or a revoked key would keep getting through for as long as sync keeps failing
  const { transport, store } = setup();
  syncWith(store, []);
  const key = newKey();
  const answer = deferred<VerifyKeyResponse>();
  transport.onVerify = () => answer.promise;
  const pending = store.check(key.key, "allow");
  syncWith(store, []);
  answer.resolve({ valid: true, key: keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_new" }), consumer: consumerRecord({ consumer: "cons_new" }) });
  await pending;
  transport.onVerify = () => ({ valid: false, reason: "not_found" });
  assert.equal((await store.check(key.key, "allow")).result, "invalid");
  assert.equal(transport.verifyCalls.length, 2);
});

test("a request still waiting when a sync revokes its key is refused", async () => {
  const { transport, store } = setup();
  syncWith(store, []);
  const key = newKey();
  const answer = deferred<VerifyKeyResponse>();
  transport.onVerify = () => answer.promise;
  const pending = store.check(key.key, "allow");
  syncWith(store, [keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_new", status: "revoked" })]);
  answer.resolve({ valid: true, key: keyRecord({ key_id: key.keyId, hash: sha256(key.key), consumer: "cons_new" }), consumer: consumerRecord({ consumer: "cons_new" }) });
  assert.equal((await pending).result, "invalid");
});
