import { test } from "node:test";
import assert from "node:assert/strict";

import {
  checksumFor,
  crc32,
  encodeBase62,
  generateApiKey,
  hasValidChecksum,
  parseApiKey,
  randomBase62,
} from "../src/key-format.ts";

test("crc32 matches the standard check value", () => {
  // The standard CRC-32 check value: "123456789" → 0xCBF43926
  assert.equal(crc32("123456789"), 0xcbf43926);
  assert.equal(crc32(""), 0);
});

test("base62 encoding pads to the requested width", () => {
  assert.equal(encodeBase62(0, 6), "000000");
  assert.equal(encodeBase62(61, 6), "00000z");
  assert.equal(encodeBase62(62, 6), "000010");
  // 2^32 − 1 fits in 6 base62 digits (62^6 ≈ 5.7 × 10^10)
  assert.equal(encodeBase62(0xffffffff, 6).length, 6);
});

test("generated keys have the documented shape and a valid checksum", () => {
  for (let index = 0; index < 200; index++) {
    const { key, keyId } = generateApiKey({ prefix: "acme", environment: "live" });
    const parsed = parseApiKey(key);
    assert.ok(parsed, key);
    assert.equal(parsed.prefix, "acme");
    assert.equal(parsed.environment, "live");
    assert.equal(parsed.keyId, keyId);
    assert.equal(key.length, "acme_live_".length + 8 + 32 + 6);
    assert.ok(hasValidChecksum(key));
  }
});

test("a single changed character fails the checksum", () => {
  const { key } = generateApiKey({ prefix: "mck", environment: "test" });
  const position = "mck_test_".length + 3;
  const replacement = key[position] === "a" ? "b" : "a";
  const tampered = key.slice(0, position) + replacement + key.slice(position + 1);
  assert.equal(hasValidChecksum(tampered), false);
});

test("malformed keys are rejected before the checksum", () => {
  assert.equal(parseApiKey("ACME_live_" + "a".repeat(46)), null);
  assert.equal(parseApiKey("acme_prod_" + "a".repeat(46)), null);
  assert.equal(parseApiKey("acme_live_" + "a".repeat(45)), null);
  assert.equal(parseApiKey("a_live_" + "a".repeat(46)), null);
  assert.equal(hasValidChecksum("not-a-key"), false);
});

test("invalid prefixes are refused when generating", () => {
  assert.throws(() => generateApiKey({ prefix: "A", environment: "live" }));
  assert.throws(() => generateApiKey({ prefix: "toolongprefix", environment: "live" }));
});

test("random base62 uses rejection sampling, so a byte stream of 248+ values is skipped", () => {
  let call = 0;
  // The first batch is all bytes that get discarded, only the second is usable
  const bytes = (length: number) => new Uint8Array(length).fill(call++ === 0 ? 250 : 61);
  assert.equal(randomBase62(4, bytes), "zzzz");
});

test("checksumFor is the base62 CRC32 of the body", () => {
  const body = "acme_live_k7Gx2PqA" + "0".repeat(32);
  assert.equal(checksumFor(body), encodeBase62(crc32(body), 6));
});
