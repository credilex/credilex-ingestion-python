"""Exception hierarchy for Credilex Ingestion SDK."""
from __future__ import annotations

from typing import Any


class IngestError(Exception):
    """Base exception per tutti gli errori SDK."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


class AuthError(IngestError):
    """Errore di autenticazione (HMAC invalid, credenziale revocata/scaduta)."""


class ValidationError(IngestError):
    """Payload rifiutato dal server — violazioni di validazione."""

    def __init__(
        self,
        message: str,
        *,
        violations: list[dict[str, Any]] | None = None,
        **kw: Any,
    ) -> None:
        super().__init__(message, **kw)
        self.violations = violations or []


class QuotaError(IngestError):
    """Rate limit o batch size limit superato."""


class NotFoundError(IngestError):
    """Risorsa non trovata (pratica, documento, session)."""


class ServerError(IngestError):
    """Errore 5xx interno server."""


class IngestConfigError(IngestError):
    """Errore di configurazione client (es. mismatch env tra client e server).

    Tipico caso: client configurato con `env="sandbox"` ma il server risponde
    con `X-Credilex-Env: prod` (o viceversa). Indica quasi sempre un base_url
    sbagliato o un mix-up di credenziali tra ambienti.
    """
