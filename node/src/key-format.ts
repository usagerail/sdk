// API key format: {prefix}_{live|test}_{8-char key ID}{32-char secret}{6-char checksum}, all base62.
// The checksum is the CRC32 of everything before it, in base62, left-padded with zeros to 6 characters. It rejects typos and made-up keys locally without a database lookup.
// The SDK's local check, the server and the key generator on usagerail.com all use this one implementation.

export const BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
export const KEY_ID_LENGTH = 8;
export const SECRET_LENGTH = 32;
export const CHECKSUM_LENGTH = 6;

export type KeyEnvironment = "live" | "test";

export type ParsedApiKey = {
  prefix: string;
  environment: KeyEnvironment;
  keyId: string;
  secret: string;
  checksum: string;
};

type RandomBytes = (length: number) => Uint8Array;

const defaultRandomBytes: RandomBytes = (length) => globalThis.crypto.getRandomValues(new Uint8Array(length));

export function isValidPrefix(prefix: string) {
  return /^[a-z0-9]{2,10}$/.test(prefix);
}

// Rejection sampling: only bytes < 248 (= 62 × 4) are taken modulo 62, which avoids modulo bias
export function randomBase62(length: number, randomBytes: RandomBytes = defaultRandomBytes) {
  let output = "";
  while (output.length < length) {
    for (const byte of randomBytes(length * 2)) {
      if (byte < 248) output += BASE62_ALPHABET[byte % 62];
      if (output.length === length) break;
    }
  }
  return output;
}

const crcTable = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

// Standard CRC-32 (IEEE 802.3). Keys are ASCII only, so each character code is one byte
export function crc32(text: string) {
  let crc = 0xffffffff;
  for (let index = 0; index < text.length; index++) {
    crc = crcTable[(crc ^ text.charCodeAt(index)) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

export function encodeBase62(value: number, width: number) {
  let remaining = value;
  let output = "";
  do {
    output = BASE62_ALPHABET[remaining % 62] + output;
    remaining = Math.floor(remaining / 62);
  } while (remaining > 0);
  return output.padStart(width, "0");
}

export function checksumFor(body: string) {
  return encodeBase62(crc32(body), CHECKSUM_LENGTH);
}

export function generateApiKey(
  options: { prefix: string; environment: KeyEnvironment },
  randomBytes: RandomBytes = defaultRandomBytes,
) {
  if (!isValidPrefix(options.prefix)) {
    throw new Error("Prefix must be 2-10 lowercase letters or digits");
  }
  const keyId = randomBase62(KEY_ID_LENGTH, randomBytes);
  const secret = randomBase62(SECRET_LENGTH, randomBytes);
  const body = `${options.prefix}_${options.environment}_${keyId}${secret}`;
  return { key: body + checksumFor(body), keyId };
}

const keyPattern = new RegExp(
  `^([a-z0-9]{2,10})_(live|test)_([0-9A-Za-z]{${KEY_ID_LENGTH}})([0-9A-Za-z]{${SECRET_LENGTH}})([0-9A-Za-z]{${CHECKSUM_LENGTH}})$`,
);

// Checks the format only, not the checksum
export function parseApiKey(key: string): ParsedApiKey | null {
  const match = keyPattern.exec(key);
  if (!match) return null;
  const [, prefix, environment, keyId, secret, checksum] = match;
  return { prefix, environment: environment as KeyEnvironment, keyId, secret, checksum };
}

// Format and checksum both valid. This is the SDK's first check: if it fails, respond 401 without touching the network
export function hasValidChecksum(key: string) {
  const parsed = parseApiKey(key);
  if (!parsed) return false;
  return checksumFor(key.slice(0, -CHECKSUM_LENGTH)) === parsed.checksum;
}
