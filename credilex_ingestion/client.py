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
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Union

import httpx

from credilex_ingestion._version import __version__
from credilex_ingestion.auth import build_signed_headers
from credilex_ingestion.errors import (
    AuthError,
    IngestConfigError,
    IngestError,
    NotFoundError,
    QuotaError,
    ServerError,
    ValidationError,
)

# Environment URLs
PROD_BASE_URL = "https://ingest.credilex.it"
SANDBOX_BASE_URL = "https://sandbox.credilex.it"
# Backward-compat alias
DEFAULT_BASE_URL = PROD_BASE_URL

# Map client env -> expected server header value (X-Credilex-Env)
_ENV_HEADER_MAP = {
    "production": "prod",
    "sandbox": "sandbox",
}
_VALID_ENVS = ("production", "sandbox")

DEFAULT_TIMEOUT = 60.0
DEFAULT_UPLOAD_TIMEOUT = 600.0  # 10 min per large R2 uploads
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
    if status == 402:
        # Payment required — credenziale a pagamento esaurita.
        return QuotaError(msg, code=code or "quota.payment_required", details=data)
    if status == 403:
        return AuthError(msg, code=code or "forbidden", details=data)
    if status == 404:
        return NotFoundError(msg, code=code, details=data)
    if status == 409:
        # Idempotency-Key conflict, session stato non valido, ecc.
        violations = data.get("violations") or []
        return ValidationError(msg, code=code or "integrity.conflict", details=data, violations=violations)
    if status == 413 or status == 429:
        return QuotaError(msg, code=code, details=data)
    if status == 415:
        return IngestError(msg, code=code or "syntax.content_type_invalid", details=data)
    if status == 422:
        violations = data.get("violations") or []
        return ValidationError(msg, code=code, details=data, violations=violations)
    if status >= 500:
        return ServerError(msg, code=code, details=data)
    return IngestError(msg, code=code, details=data)


class _ClientBase:
    """Shared config + URL builder."""

    # Class-level constants (also exposed as `IngestClient.PROD_BASE_URL` etc.)
    PROD_BASE_URL = PROD_BASE_URL
    SANDBOX_BASE_URL = SANDBOX_BASE_URL

    def __init__(
        self,
        *,
        api_id: str,
        api_secret: str,
        env: str = "production",
        base_url: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        user_agent: Optional[str] = None,
    ):
        if not api_id or not api_secret:
            raise ValueError("api_id and api_secret are required")
        if env not in _VALID_ENVS:
            raise ValueError(
                f"env must be 'production' or 'sandbox', got {env!r}"
            )
        self.api_id = api_id
        self.api_secret = api_secret
        self.env = env
        # base_url= esplicito vince sempre (custom enterprise endpoint).
        # Altrimenti deriva dall'env.
        if base_url is not None:
            resolved_base = base_url
        elif env == "sandbox":
            resolved_base = SANDBOX_BASE_URL
        else:  # production
            resolved_base = PROD_BASE_URL
        self.base_url = resolved_base.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.user_agent = user_agent or f"credilex-ingestion-python/{__version__}"

    def _expected_env_header(self) -> str:
        """Valore atteso dell'header `X-Credilex-Env` nelle response server."""
        return _ENV_HEADER_MAP[self.env]

    def _verify_env_header(self, resp: httpx.Response) -> None:
        """Verifica che `X-Credilex-Env` (se presente su 2xx) matchi l'env client.

        Skip volutamente:
        - response 4xx/5xx (es. 401 da nginx, 502 da gateway senza header)
        - response 2xx senza header (compat con server vecchi)

        Raise `IngestConfigError` solo quando il server lo dichiara esplicitamente
        e il valore non matcha — sintomo chiaro di base_url/env sbagliato.
        """
        if not (200 <= resp.status_code < 300 or resp.status_code == 207):
            return
        server_env = resp.headers.get("X-Credilex-Env")
        if server_env is None:
            return
        expected = self._expected_env_header()
        if server_env.strip().lower() != expected:
            raise IngestConfigError(
                f"environment mismatch: expected {expected!r} "
                f"(client env={self.env!r}) but server returned {server_env!r} "
                f"on {self.base_url} — check your base_url / env parameter",
                code="config.env_mismatch",
                details={
                    "client_env": self.env,
                    "expected_header": expected,
                    "server_header": server_env,
                    "base_url": self.base_url,
                },
            )

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
            self._verify_env_header(resp)
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
            self._verify_env_header(resp)
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

    def put_to_r2(
        self,
        presigned_put_url: str,
        file_bytes: Optional[bytes] = None,
        *,
        file_path: Optional[Union[str, Path]] = None,
        file_obj: Optional[BinaryIO] = None,
        content_type: str = "application/octet-stream",
        timeout: float = DEFAULT_UPLOAD_TIMEOUT,
    ) -> None:
        """PUT diretto su R2 usando il presigned URL. Ritorna None, raise su errore.

        Supporta 3 modi di fornire il contenuto (esattamente uno):
            - file_bytes: bytes in memoria (small files <100MB)
            - file_path: stream da disco (large files — no OOM)
            - file_obj: stream da file-like object (deve supportare seek/tell)

        Per backward-compat, `file_bytes` resta accettato come secondo argomento
        posizionale. `timeout` default 600s per tollerare upload grandi.
        """
        sources_provided = sum(x is not None for x in (file_bytes, file_path, file_obj))
        if sources_provided != 1:
            raise ValueError(
                "fornisci esattamente uno tra file_bytes, file_path, file_obj"
            )

        if file_path is not None:
            p = Path(file_path)
            size = p.stat().st_size
            with p.open("rb") as f:
                resp = self._http.put(
                    presigned_put_url,
                    content=f,
                    headers={"Content-Type": content_type, "Content-Length": str(size)},
                    timeout=timeout,
                )
        elif file_obj is not None:
            if not (hasattr(file_obj, "seek") and hasattr(file_obj, "tell")):
                raise ValueError(
                    "file_obj deve supportare seek/tell per calcolare Content-Length"
                )
            file_obj.seek(0, 2)  # end
            size = file_obj.tell()
            file_obj.seek(0)
            resp = self._http.put(
                presigned_put_url,
                content=file_obj,
                headers={"Content-Type": content_type, "Content-Length": str(size)},
                timeout=timeout,
            )
        else:  # file_bytes
            assert file_bytes is not None  # narrowing
            resp = self._http.put(
                presigned_put_url,
                content=file_bytes,
                headers={"Content-Type": content_type, "Content-Length": str(len(file_bytes))},
                timeout=timeout,
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
            self._verify_env_header(resp)
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

    async def upload_batch(
        self,
        *,
        pratiche: List[dict],
        batch_index: Optional[int] = None,
        batch_total: Optional[int] = None,
        batch_sha256: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Upload batch fino a 500 pratiche. Ritorna 207 Multi-Status response.

        Signature allineata a `IngestClient.upload_batch` (kwargs espliciti,
        non più **kwargs generico).
        """
        body: Dict[str, Any] = {"pratiche": pratiche}
        if batch_index is not None:
            body["batch_index"] = batch_index
        if batch_total is not None:
            body["batch_total"] = batch_total
        if batch_sha256 is not None:
            body["batch_sha256"] = batch_sha256
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
            self._verify_env_header(resp)
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

    async def put_to_r2(
        self,
        presigned_put_url: str,
        file_bytes: Optional[bytes] = None,
        *,
        file_path: Optional[Union[str, Path]] = None,
        file_obj: Optional[BinaryIO] = None,
        content_type: str = "application/octet-stream",
        timeout: float = DEFAULT_UPLOAD_TIMEOUT,
    ) -> None:
        """PUT diretto su R2 usando il presigned URL. Ritorna None, raise su errore.

        Stesso contract di `IngestClient.put_to_r2`: esattamente uno tra
        `file_bytes`, `file_path`, `file_obj`.

        TRADE-OFF ASYNC: httpx.AsyncClient non accetta file handle sync come
        content (richiede AsyncByteStream). Per evitare una dipendenza hard su
        `aiofiles`, leggiamo il file in memoria prima dell'upload anche per
        `file_path`/`file_obj`. Il risparmio RAM di streaming è quindi limitato
        al sync client. Per upload multi-GB async, leggi chunk con `aiofiles`
        e pre-serializza, oppure usa `IngestClient` sync.
        """
        sources_provided = sum(x is not None for x in (file_bytes, file_path, file_obj))
        if sources_provided != 1:
            raise ValueError(
                "fornisci esattamente uno tra file_bytes, file_path, file_obj"
            )

        if file_path is not None:
            p = Path(file_path)
            size = p.stat().st_size
            # async trade-off: leggi bytes (no aiofiles hard dep)
            payload = p.read_bytes()
        elif file_obj is not None:
            if not (hasattr(file_obj, "seek") and hasattr(file_obj, "tell")
                    and hasattr(file_obj, "read")):
                raise ValueError(
                    "file_obj deve supportare seek/tell/read per calcolare Content-Length"
                )
            file_obj.seek(0, 2)
            size = file_obj.tell()
            file_obj.seek(0)
            payload = file_obj.read()
        else:
            assert file_bytes is not None
            payload = file_bytes
            size = len(file_bytes)

        resp = await self._http.put(
            presigned_put_url,
            content=payload,
            headers={"Content-Type": content_type, "Content-Length": str(size)},
            timeout=timeout,
        )

        if resp.status_code >= 400:
            raise IngestError(f"R2 upload failed: HTTP {resp.status_code} -- {resp.text[:500]}")
