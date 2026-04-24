"""HTTP client for Credilex Ingestion API.

Due varianti:
- `IngestClient` -- sync (httpx.Client)
- `AsyncIngestClient` -- async (httpx.AsyncClient)

Entrambe espongono gli stessi metodi pubblici:
- validate_pratica(payload) -> dict
- validate_batch(payloads) -> dict
- upload_batch(pratiche=, batch_index=, batch_total=, idempotency_key=) -> dict
- get_pratica(external_id) -> dict
- get_summary() -> dict
- download_errors_ndjson() -> str (raw NDJSON content)
- documents_prepare(external_id_pratica, documents) -> list[dict]
- documents_commit(external_id_pratica, document_id) -> dict
- list_documents(external_id_pratica) -> dict
- put_to_r2(presigned_url, file_bytes) -> None  # helper diretto R2
- health() -> dict

Retry: 3 tentativi su 5xx/429 con exponential backoff (1s -> 2s -> 4s).
Timeout: 60s default per request.
"""
from __future__ import annotations

import json as json_mod
import time
from typing import Any, Dict, List, Optional

import httpx

from credilex_ingestion._version import __version__
from credilex_ingestion.auth import build_signed_headers
from credilex_ingestion.errors import (
    AuthError,
    IngestError,
    NotFoundError,
    QuotaError,
    ServerError,
    ValidationError,
)

DEFAULT_BASE_URL = "https://ingest.credilex.it"
DEFAULT_TIMEOUT = 60.0
DEFAULT_MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # seconds
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def _parse_error_response(status: int, body_text: str) -> IngestError:
    """Mappa status+body a exception tipizzata."""
    try:
        data = json_mod.loads(body_text)
    except (ValueError, TypeError):
        data = {"error": {"message": body_text[:500]}}

    err = data.get("error") or {}
    detail = data.get("detail") or {}
    code = err.get("code") or (detail.get("code") if isinstance(detail, dict) else None)
    msg = (err.get("message")
           or (detail.get("message") if isinstance(detail, dict) else None)
           or data.get("message")
           or f"HTTP {status}")

    if status == 401:
        return AuthError(msg, code=code, details=data)
    if status == 403:
        return AuthError(msg, code=code or "forbidden", details=data)
    if status == 404:
        return NotFoundError(msg, code=code, details=data)
    if status == 413 or status == 429:
        return QuotaError(msg, code=code, details=data)
    if status == 422:
        violations = data.get("violations") or []
        return ValidationError(msg, code=code, details=data, violations=violations)
    if status >= 500:
        return ServerError(msg, code=code, details=data)
    return IngestError(msg, code=code, details=data)


class _ClientBase:
    """Shared config + URL builder."""

    def __init__(
        self,
        *,
        api_id: str,
        api_secret: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        user_agent: Optional[str] = None,
    ):
        if not api_id or not api_secret:
            raise ValueError("api_id and api_secret are required")
        self.api_id = api_id
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.user_agent = user_agent or f"credilex-ingestion-python/{__version__}"

    def _sign(self, method: str, path: str, query: str, body: bytes) -> Dict[str, str]:
        hdrs = build_signed_headers(
            api_id=self.api_id, api_secret=self.api_secret,
            method=method, path=path, query=query, body=body,
        )
        hdrs["User-Agent"] = self.user_agent
        hdrs["Content-Type"] = "application/json"
        return hdrs


class IngestClient(_ClientBase):
    """Sync client per Credilex Ingestion API."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._http = httpx.Client(timeout=self.timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Any] = None,
        query: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        auth: bool = True,
    ) -> httpx.Response:
        """Internal request with HMAC signing + retry."""
        body: bytes = b""
        if json is not None:
            body = json_mod.dumps(json, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        query_str = ""
        if query:
            from urllib.parse import urlencode
            query_str = urlencode(query, doseq=True)

        headers: Dict[str, str] = {"User-Agent": self.user_agent}
        if auth:
            headers.update(self._sign(method, path, query_str, body))
            headers["Content-Type"] = "application/json"
        if extra_headers:
            headers.update(extra_headers)

        url = self.base_url + path
        if query_str:
            url = f"{url}?{query_str}"

        for attempt in range(self.max_retries + 1):
            try:
                resp = self._http.request(method, url, content=body if body else None, headers=headers)
            except httpx.TransportError as exc:
                if attempt >= self.max_retries:
                    raise IngestError(f"network error: {exc}") from exc
                time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue

            if resp.status_code in RETRY_STATUS_CODES and attempt < self.max_retries:
                time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue
            return resp

        raise IngestError("retry exhausted")

    def _handle(self, resp: httpx.Response) -> Any:
        """200-299 -> json; altrimenti -> raise tipizzato."""
        if 200 <= resp.status_code < 300 or resp.status_code == 207:
            try:
                return resp.json()
            except json_mod.JSONDecodeError:
                return resp.text
        raise _parse_error_response(resp.status_code, resp.text)

    # -- Public API ------------------------------------------------------

    def health(self) -> dict:
        resp = self._request("GET", "/health", auth=False)
        return self._handle(resp)

    def validate_pratica(self, payload: dict) -> dict:
        """Dry-run validation di un singolo PraticaPayload (no auth in M1)."""
        resp = self._request("POST", "/ingest/v1/pratiche:validate", json=payload, auth=False)
        return self._handle(resp)

    def validate_batch(self, payloads: List[dict]) -> dict:
        resp = self._request("POST", "/ingest/v1/pratiche:batch:validate", json=payloads, auth=False)
        return self._handle(resp)

    def upload_batch(
        self,
        *,
        pratiche: List[dict],
        batch_index: Optional[int] = None,
        batch_total: Optional[int] = None,
        batch_sha256: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Upload batch fino a 500 pratiche. Ritorna 207 Multi-Status response."""
        body: Dict[str, Any] = {"pratiche": pratiche}
        if batch_index is not None:
            body["batch_index"] = batch_index
        if batch_total is not None:
            body["batch_total"] = batch_total
        if batch_sha256 is not None:
            body["batch_sha256"] = batch_sha256

        extra: Dict[str, str] = {}
        if idempotency_key:
            extra["Idempotency-Key"] = idempotency_key
        resp = self._request("POST", "/ingest/v1/pratiche:batch", json=body, extra_headers=extra)
        return self._handle(resp)

    def get_pratica(self, external_id: str) -> dict:
        resp = self._request("GET", f"/ingest/v1/pratiche/{external_id}")
        return self._handle(resp)

    def get_summary(self) -> dict:
        resp = self._request("GET", "/ingest/v1/pratiche/summary")
        return self._handle(resp)

    def download_errors_ndjson(self) -> str:
        resp = self._request("GET", "/ingest/v1/pratiche/errors.ndjson")
        if 200 <= resp.status_code < 300:
            return resp.text
        raise _parse_error_response(resp.status_code, resp.text)

    def documents_prepare(
        self,
        external_id_pratica: str,
        documents: List[dict],
    ) -> List[dict]:
        """Richiede presigned PUT URL per N documenti di una pratica."""
        body = {"documents": documents}
        resp = self._request("POST", f"/ingest/v1/pratiche/{external_id_pratica}/documents:prepare", json=body)
        data = self._handle(resp)
        return data.get("uploads", [])

    def documents_commit(self, external_id_pratica: str, document_id: str, *, client_sha256: Optional[str] = None) -> dict:
        body: Dict[str, Any] = {}
        if client_sha256:
            body["client_reported_sha256"] = client_sha256
        resp = self._request("POST", f"/ingest/v1/pratiche/{external_id_pratica}/documents/{document_id}:commit", json=body)
        return self._handle(resp)

    def list_documents(self, external_id_pratica: str) -> dict:
        resp = self._request("GET", f"/ingest/v1/pratiche/{external_id_pratica}/documents")
        return self._handle(resp)

    def put_to_r2(self, presigned_put_url: str, file_bytes: bytes, *, content_type: str = "application/octet-stream") -> None:
        """PUT diretto su R2 usando il presigned URL. Ritorna None, raise su errore."""
        resp = self._http.put(
            presigned_put_url,
            content=file_bytes,
            headers={"Content-Type": content_type, "Content-Length": str(len(file_bytes))},
        )
        if resp.status_code >= 400:
            raise IngestError(f"R2 upload failed: HTTP {resp.status_code} -- {resp.text[:500]}")


class AsyncIngestClient(_ClientBase):
    """Async client per Credilex Ingestion API (httpx.AsyncClient)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._http = httpx.AsyncClient(timeout=self.timeout)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def _request(
        self, method: str, path: str, *,
        json: Optional[Any] = None, query: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None, auth: bool = True,
    ) -> httpx.Response:
        body = b""
        if json is not None:
            body = json_mod.dumps(json, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        query_str = ""
        if query:
            from urllib.parse import urlencode
            query_str = urlencode(query, doseq=True)
        headers: Dict[str, str] = {"User-Agent": self.user_agent}
        if auth:
            headers.update(self._sign(method, path, query_str, body))
            headers["Content-Type"] = "application/json"
        if extra_headers:
            headers.update(extra_headers)
        url = self.base_url + path
        if query_str:
            url = f"{url}?{query_str}"

        import asyncio
        for attempt in range(self.max_retries + 1):
            try:
                resp = await self._http.request(method, url, content=body if body else None, headers=headers)
            except httpx.TransportError as exc:
                if attempt >= self.max_retries:
                    raise IngestError(f"network error: {exc}") from exc
                await asyncio.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue
            if resp.status_code in RETRY_STATUS_CODES and attempt < self.max_retries:
                await asyncio.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue
            return resp
        raise IngestError("retry exhausted")

    def _handle(self, resp: httpx.Response) -> Any:
        if 200 <= resp.status_code < 300 or resp.status_code == 207:
            try:
                return resp.json()
            except json_mod.JSONDecodeError:
                return resp.text
        raise _parse_error_response(resp.status_code, resp.text)

    async def health(self) -> dict:
        resp = await self._request("GET", "/health", auth=False)
        return self._handle(resp)

    async def validate_pratica(self, payload: dict) -> dict:
        resp = await self._request("POST", "/ingest/v1/pratiche:validate", json=payload, auth=False)
        return self._handle(resp)

    async def validate_batch(self, payloads: List[dict]) -> dict:
        resp = await self._request("POST", "/ingest/v1/pratiche:batch:validate", json=payloads, auth=False)
        return self._handle(resp)

    async def upload_batch(self, *, pratiche: List[dict],
                           idempotency_key: Optional[str] = None, **kwargs) -> dict:
        body: Dict[str, Any] = {"pratiche": pratiche, **{k: v for k, v in kwargs.items() if v is not None}}
        extra = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        resp = await self._request("POST", "/ingest/v1/pratiche:batch", json=body, extra_headers=extra)
        return self._handle(resp)

    async def get_pratica(self, external_id: str) -> dict:
        resp = await self._request("GET", f"/ingest/v1/pratiche/{external_id}")
        return self._handle(resp)

    async def get_summary(self) -> dict:
        resp = await self._request("GET", "/ingest/v1/pratiche/summary")
        return self._handle(resp)

    async def download_errors_ndjson(self) -> str:
        resp = await self._request("GET", "/ingest/v1/pratiche/errors.ndjson")
        if 200 <= resp.status_code < 300:
            return resp.text
        raise _parse_error_response(resp.status_code, resp.text)

    async def documents_prepare(self, external_id_pratica: str, documents: List[dict]) -> List[dict]:
        resp = await self._request("POST", f"/ingest/v1/pratiche/{external_id_pratica}/documents:prepare", json={"documents": documents})
        data = self._handle(resp)
        return data.get("uploads", [])

    async def documents_commit(self, external_id_pratica: str, document_id: str, *, client_sha256: Optional[str] = None) -> dict:
        body: Dict[str, Any] = {}
        if client_sha256:
            body["client_reported_sha256"] = client_sha256
        resp = await self._request("POST", f"/ingest/v1/pratiche/{external_id_pratica}/documents/{document_id}:commit", json=body)
        return self._handle(resp)

    async def list_documents(self, external_id_pratica: str) -> dict:
        resp = await self._request("GET", f"/ingest/v1/pratiche/{external_id_pratica}/documents")
        return self._handle(resp)

    async def put_to_r2(self, presigned_put_url: str, file_bytes: bytes, *, content_type: str = "application/octet-stream") -> None:
        resp = await self._http.put(presigned_put_url, content=file_bytes,
                                    headers={"Content-Type": content_type, "Content-Length": str(len(file_bytes))})
        if resp.status_code >= 400:
            raise IngestError(f"R2 upload failed: HTTP {resp.status_code}")
