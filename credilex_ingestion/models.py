"""Type helpers for Credilex Ingestion API payloads.

Fornisce TypedDict compatibili Python 3.8+ per i tipi principali del contratto
API (Pratica, Soggetto, Documento, BatchRequest, ecc.). Sono solo hint di tipo —
non eseguono validazione runtime (per quella serve Pydantic o fare la call
:validate server-side).

Se pydantic è installato (`pip install credilex-ingestion[pydantic]`), si può
opzionalmente importare e usare i modelli Pydantic client-side per validation.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict


class PraticaDict(TypedDict, total=False):
    """Minimal TypedDict per la Pratica (i campi obbligatori sono marcati NotRequired in total=False)."""
    external_id: str
    tipo_portafoglio: Literal["npl", "utp", "utility", "cessione"]
    prodotto: str
    istituto_cedente: str
    importo_capitale_residuo: str
    importo_totale: str
    valuta: str
    # Tutti gli altri campi (~150) sono opzionali — consulta la OpenAPI spec


class SoggettoDict(TypedDict, total=False):
    external_id: str
    ruolo: Literal["debitore_principale", "coobbligato_solidale", "garante_fideiussore", "terzo_datore_ipoteca", "erede"]
    tipo: Literal["persona_fisica", "persona_giuridica", "ditta_individuale"]


class GaranziaDict(TypedDict, total=False):
    external_id: str
    tipo: str


class ImmobileDict(TypedDict, total=False):
    external_id: str
    tipo: str


class DocumentoDict(TypedDict, total=False):
    external_id: str
    tipo_documento: str
    mime: str
    size_bytes: int
    sha256_expected: str
    pratica_external_id: Optional[str]


class PraticaPayloadDict(TypedDict, total=False):
    """Wrapper completo del payload per una pratica (inviato a :batch o :validate)."""
    pratica: PraticaDict
    soggetti: List[SoggettoDict]
    garanzie: List[GaranziaDict]
    immobili: List[ImmobileDict]
    documenti: List[DocumentoDict]


class BatchUploadResponseDict(TypedDict, total=False):
    session_id: str
    batch_size: int
    total_accepted: int
    total_rejected: int
    accepted: List[Dict[str, Any]]
    rejected: List[Dict[str, Any]]
    request_id: str


class ValidateResponseDict(TypedDict, total=False):
    external_id: str
    valid: bool
    violations: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]


class PresignedUrlDict(TypedDict, total=False):
    document_id: str
    external_id: str
    presigned_put_url: str
    expires_at: str
    expected_sha256: str
    r2_key: str


# --- Optional pydantic bridge ---------------------------------------------
def load_pydantic_models():
    """Se pydantic è installato, ritorna modelli Pydantic per validation client-side.

    Altrimenti UserWarning + ritorna None.

    Uso:
        from credilex_ingestion.models import load_pydantic_models
        models = load_pydantic_models()
        if models:
            Pratica = models["Pratica"]
            p = Pratica.model_validate(payload)  # raise ValidationError su invalid
    """
    try:
        from pydantic import BaseModel, Field
    except ImportError:
        import warnings
        warnings.warn(
            "pydantic not installed — install with 'pip install credilex-ingestion[pydantic]'",
            stacklevel=2,
        )
        return None

    # Stub minimale — per i modelli full serve download dell'OpenAPI spec
    # e runtime code generation. Per M6 skeleton, ritorna wrapper base.
    class _BasePratica(BaseModel):
        external_id: str
        tipo_portafoglio: str
        prodotto: str
        istituto_cedente: str
        importo_capitale_residuo: str
        importo_totale: str
        valuta: str = "EUR"
        model_config = {"extra": "allow"}  # lascia passare i ~150 campi non modellati

    return {"Pratica": _BasePratica}
