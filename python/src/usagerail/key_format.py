"""API key format: {prefix}_{live|test}_{8-char key ID}{32-char secret}{6-char checksum}, all base62.

The checksum is the CRC32 of everything before it, in base62, left-padded with zeros to 6 characters. It rejects typos and made-up keys locally without a database lookup.
Same algorithm as key-format.ts in the Node SDK; both generate and verify identically (tests/test_key_format.py has shared test vectors).
"""

from __future__ import annotations

import re
import secrets
import zlib
from typing import Callable, Literal, NamedTuple

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
KEY_ID_LENGTH = 8
SECRET_LENGTH = 32
CHECKSUM_LENGTH = 6

KeyEnvironment = Literal["live", "test"]
RandomBytes = Callable[[int], bytes]


class ParsedApiKey(NamedTuple):
    prefix: str
    environment: KeyEnvironment
    key_id: str
    secret: str
    checksum: str


class GeneratedKey(NamedTuple):
    key: str
    key_id: str


_PREFIX = re.compile(r"[a-z0-9]{2,10}")
_KEY = re.compile(
    rf"([a-z0-9]{{2,10}})_(live|test)_([0-9A-Za-z]{{{KEY_ID_LENGTH}}})([0-9A-Za-z]{{{SECRET_LENGTH}}})([0-9A-Za-z]{{{CHECKSUM_LENGTH}}})"
)


def is_valid_prefix(prefix: str) -> bool:
    return _PREFIX.fullmatch(prefix) is not None


def random_base62(length: int, random_bytes: RandomBytes = secrets.token_bytes) -> str:
    """Rejection sampling: only bytes < 248 (= 62 × 4) are taken modulo 62, which avoids modulo bias"""
    output: list[str] = []
    while len(output) < length:
        for byte in random_bytes(length * 2):
            if byte < 248:
                output.append(BASE62_ALPHABET[byte % 62])
            if len(output) == length:
                break
    return "".join(output)


def crc32(text: str) -> int:
    """Standard CRC-32 (IEEE 802.3), same as zlib's. Keys are ASCII only"""
    return zlib.crc32(text.encode("ascii")) & 0xFFFFFFFF


def encode_base62(value: int, width: int) -> str:
    digits: list[str] = []
    remaining = value
    while True:
        remaining, digit = divmod(remaining, 62)
        digits.append(BASE62_ALPHABET[digit])
        if remaining == 0:
            break
    return "".join(reversed(digits)).rjust(width, "0")


def checksum_for(body: str) -> str:
    return encode_base62(crc32(body), CHECKSUM_LENGTH)


def generate_api_key(prefix: str, environment: KeyEnvironment, random_bytes: RandomBytes = secrets.token_bytes) -> GeneratedKey:
    if not is_valid_prefix(prefix):
        raise ValueError("Prefix must be 2-10 lowercase letters or digits")
    key_id = random_base62(KEY_ID_LENGTH, random_bytes)
    secret = random_base62(SECRET_LENGTH, random_bytes)
    body = f"{prefix}_{environment}_{key_id}{secret}"
    return GeneratedKey(body + checksum_for(body), key_id)


def parse_api_key(key: str) -> ParsedApiKey | None:
    """Checks the format only, not the checksum"""
    match = _KEY.fullmatch(key)
    if match is None:
        return None
    prefix, environment, key_id, secret, checksum = match.groups()
    return ParsedApiKey(prefix, environment, key_id, secret, checksum)  # type: ignore[arg-type]


def has_valid_checksum(key: str) -> bool:
    """Format and checksum both valid. This is the SDK's first check: if it fails, respond 401 without touching the network"""
    parsed = parse_api_key(key)
    return parsed is not None and checksum_for(key[:-CHECKSUM_LENGTH]) == parsed.checksum
