"""HMAC-SHA256 request signing for Credilex Ingestion API.

Ogni richiesta verso endpoint protetti richiede i 5 header:
- X-Credilex-Api-Id
- X-Credilex-Timestamp (ISO 8601 UTC with trailing Z)
- X-Credilex-Nonce (uuid4)
- X-Credilex-Content-Sha256 (hex)
- X-Credilex-Signature (hex HMAC-SHA256)

Uso:
    from credilex_ingestion.auth import build_signed_headers

    headers = build_signed_headers(
        api_id="clx_id_live_abc...",
        api_secret="clx_sk_live_xyz...",
        method="POST",
        path="/ingest/v1/pratiche:batch",
        query="",
        body=b'{"pratiche": [...]}',
    )
    # headers e' dict da passare a httpx.post(..., headers=headers)
"""
from __future__ import annotations

import hashlib
import hmac
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.parse import parse_qsl, urlencode


def iso_utc_timestamp() -> str:
    """Genera timestamp ISO 8601 UTC con trailing Z (secondo precisione)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_nonce() -> str:
    """Genera nonce unico (uuid4 hex string)."""
    return str(uuid.uuid4())


def body_sha256_hex(body: bytes) -> str:
    """SHA-256 hex lowercase del body bytes. Empty body -> hash di bytes vuoti."""
    return hashlib.sha256(body).hexdigest()


def sort_query(query: str) -> str:
    """Ordina alfabeticamente i query params e li ri-encoda.

    Stringa vuota -> ritorna stringa vuota.
    """
    if not query:
        return ""
    items = sorted(parse_qsl(query, keep_blank_values=True))
    return urlencode(items, doseq=True)


def canonical_string(
    *,
    method: str,
    path: str,
    sorted_query: str,
    content_sha256: str,
    timestamp: str,
    nonce: str,
) -> str:
    """Costruisce la canonical string per la firma HMAC.

    Formato (6 righe separate da \\n):
        METHOD
        PATH
        SORTED_QUERY_STRING
        CONTENT_SHA256_HEX
        TIMESTAMP
        NONCE

    METHOD e' uppercase, path include leading slash, query string URL-encoded.
    """
    return "\n".join([
        method.upper(),
        path,
        sorted_query,
        content_sha256,
        timestamp,
        nonce,
    ])


def compute_signature(secret: str, canonical: str) -> str:
    """HMAC-SHA256(secret, canonical) -> hex lowercase."""
    return hmac.new(
        key=secret.encode("utf-8"),
        msg=canonical.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()


def build_signed_headers(
    *,
    api_id: str,
    api_secret: str,
    method: str,
    path: str,
    query: str = "",
    body: bytes = b"",
    timestamp: Optional[str] = None,
    nonce: Optional[str] = None,
) -> Dict[str, str]:
    """Costruisce i 5 header HMAC completi per una richiesta.

    Args:
        api_id: api_id pubblico (clx_id_...)
        api_secret: api_secret segreto (clx_sk_...)
        method: HTTP method (GET, POST, etc)
        path: URL path (es. /ingest/v1/pratiche:batch)
        query: query string raw (es. "foo=bar&baz=qux"), default vuota
        body: body bytes (default b"" per GET/DELETE)
        timestamp: override timestamp (testabilita'); default now()
        nonce: override nonce (testabilita'); default uuid4()

    Returns:
        dict con i 5 header (X-Credilex-Api-Id, X-Credilex-Timestamp,
        X-Credilex-Nonce, X-Credilex-Content-Sha256, X-Credilex-Signature).
    """
    ts = timestamp or iso_utc_timestamp()
    n = nonce or generate_nonce()
    content_sha = body_sha256_hex(body)
    canonical = canonical_string(
        method=method, path=path, sorted_query=sort_query(query),
        content_sha256=content_sha, timestamp=ts, nonce=n,
    )
    sig = compute_signature(api_secret, canonical)
    return {
        "X-Credilex-Api-Id": api_id,
        "X-Credilex-Timestamp": ts,
        "X-Credilex-Nonce": n,
        "X-Credilex-Content-Sha256": content_sha,
        "X-Credilex-Signature": sig,
    }
