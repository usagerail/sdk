import pytest

from usagerail.key_format import checksum_for, crc32, encode_base62, generate_api_key, has_valid_checksum, parse_api_key, random_base62


def test_crc32_matches_the_standard_check_value():
    # The standard CRC-32 check value: "123456789" → 0xCBF43926
    assert crc32("123456789") == 0xCBF43926
    assert crc32("") == 0


def test_checksums_match_the_node_sdk():
    # These two were computed with the Node SDK's key-format.ts: both sides must generate and verify keys identically
    assert crc32("acme_live_k7Gx2PqA" + "0" * 32) == 1726558099
    assert checksum_for("acme_live_k7Gx2PqA" + "0" * 32) == "1sqSdH"
    assert checksum_for("mck_test_AAAAAAAA" + "zZ9" * 10 + "xy") == "2erkPJ"


def test_base62_encoding_pads_to_the_requested_width():
    assert encode_base62(0, 6) == "000000"
    assert encode_base62(61, 6) == "00000z"
    assert encode_base62(62, 6) == "000010"
    # 2^32 − 1 fits in 6 base62 digits (62^6 ≈ 5.7 × 10^10)
    assert len(encode_base62(0xFFFFFFFF, 6)) == 6


def test_generated_keys_have_the_documented_shape_and_a_valid_checksum():
    for _ in range(200):
        key, key_id = generate_api_key("acme", "live")
        parsed = parse_api_key(key)
        assert parsed is not None, key
        assert parsed.prefix == "acme"
        assert parsed.environment == "live"
        assert parsed.key_id == key_id
        assert len(key) == len("acme_live_") + 8 + 32 + 6
        assert has_valid_checksum(key)


def test_a_single_changed_character_fails_the_checksum():
    key = generate_api_key("mck", "test").key
    position = len("mck_test_") + 3
    replacement = "b" if key[position] == "a" else "a"
    assert not has_valid_checksum(key[:position] + replacement + key[position + 1 :])


def test_malformed_keys_are_rejected_before_the_checksum():
    assert parse_api_key("ACME_live_" + "a" * 46) is None
    assert parse_api_key("acme_prod_" + "a" * 46) is None
    assert parse_api_key("acme_live_" + "a" * 45) is None
    assert parse_api_key("a_live_" + "a" * 46) is None
    # A trailing newline doesn't pass either (fullmatch, not $)
    assert parse_api_key(generate_api_key("acme", "live").key + "\n") is None
    assert not has_valid_checksum("not-a-key")


def test_invalid_prefixes_are_refused_when_generating():
    with pytest.raises(ValueError):
        generate_api_key("A", "live")
    with pytest.raises(ValueError):
        generate_api_key("toolongprefix", "live")


def test_random_base62_skips_bytes_of_248_and_above():
    calls = []

    def random_bytes(length: int) -> bytes:
        calls.append(length)
        # The first batch is all bytes that get discarded, only the second is usable
        return bytes([250 if len(calls) == 1 else 61] * length)

    assert random_base62(4, random_bytes) == "zzzz"
