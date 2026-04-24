"""Unit test per HMAC signing module."""
import pytest

from credilex_ingestion.auth import (
    body_sha256_hex,
    build_signed_headers,
    canonical_string,
    compute_signature,
    generate_nonce,
    iso_utc_timestamp,
    sort_query,
)


class TestCanonicalString:
    def test_format(self):
        out = canonical_string(
            method="POST", path="/ingest/v1/pratiche:batch",
            sorted_query="", content_sha256="abc", timestamp="2026-04-24T10:00:00Z", nonce="n1",
        )
        assert out.split("\n") == ["POST", "/ingest/v1/pratiche:batch", "", "abc", "2026-04-24T10:00:00Z", "n1"]

    def test_uppercase_method(self):
        out = canonical_string(method="post", path="/x", sorted_query="", content_sha256="", timestamp="t", nonce="n")
        assert out.startswith("POST\n")


class TestSortQuery:
    def test_empty(self):
        assert sort_query("") == ""

    def test_already_sorted(self):
        assert sort_query("a=1") == "a=1"

    def test_unsorted(self):
        assert sort_query("b=2&a=1") == "a=1&b=2"

    def test_multiple_values(self):
        assert sort_query("b=2&a=1&b=3") in ("a=1&b=2&b=3", "a=1&b=3&b=2")  # order tra duplicati implementation-dep


class TestBodySha:
    def test_empty(self):
        assert body_sha256_hex(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_hello(self):
        assert body_sha256_hex(b"hello") == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"


class TestSignature:
    def test_deterministic(self):
        sig1 = compute_signature("secret", "canonical")
        sig2 = compute_signature("secret", "canonical")
        assert sig1 == sig2
        assert len(sig1) == 64  # hex sha256

    def test_different_secret(self):
        s1 = compute_signature("s1", "c")
        s2 = compute_signature("s2", "c")
        assert s1 != s2


class TestTimestamp:
    def test_iso_format(self):
        ts = iso_utc_timestamp()
        assert ts.endswith("Z")
        assert "T" in ts
        assert len(ts) == 20  # 2026-04-24T10:00:00Z


class TestNonce:
    def test_unique(self):
        nonces = {generate_nonce() for _ in range(100)}
        assert len(nonces) == 100


class TestBuildSignedHeaders:
    def test_all_headers_present(self):
        hdrs = build_signed_headers(
            api_id="clx_id_test_xyz", api_secret="clx_sk_test_sec",
            method="POST", path="/test", body=b"{}",
        )
        assert "X-Credilex-Api-Id" in hdrs
        assert "X-Credilex-Timestamp" in hdrs
        assert "X-Credilex-Nonce" in hdrs
        assert "X-Credilex-Content-Sha256" in hdrs
        assert "X-Credilex-Signature" in hdrs
        assert hdrs["X-Credilex-Api-Id"] == "clx_id_test_xyz"
        assert len(hdrs["X-Credilex-Signature"]) == 64

    def test_deterministic_with_fixed_ts_nonce(self):
        args = dict(
            api_id="i", api_secret="s", method="GET", path="/x",
            timestamp="2026-04-24T10:00:00Z", nonce="n1",
        )
        h1 = build_signed_headers(**args)
        h2 = build_signed_headers(**args)
        assert h1 == h2
