"""High-level document upload helper.

Fornisce `upload_document` che fa in una sola chiamata:
1. Legge il file bytes
2. Calcola SHA256
3. Chiama documents:prepare → ottiene presigned URL
4. Upload diretto su R2
5. Chiama documents:commit per triggerare verify+ClamAV server-side

Uso:
    from credilex_ingestion import IngestClient
    from credilex_ingestion.documents import upload_document

    client = IngestClient(api_id=..., api_secret=...)
    result = upload_document(
        client,
        external_id_pratica="ACME-001",
        external_document_id="DOC-001",
        tipo_documento="contratto_originario",
        file_path="/path/to/contratto.pdf",
        mime="application/pdf",
    )
    print(f"Document {result['document_id']} state: {result['state']}")
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from credilex_ingestion.client import IngestClient
from credilex_ingestion.errors import IngestError


def _sha256_file(file_path: str) -> str:
    """Calcola SHA256 hex di un file, streaming 64KB chunks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def upload_document(
    client: IngestClient,
    *,
    external_id_pratica: str,
    external_document_id: str,
    tipo_documento: str,
    file_path: Optional[str] = None,
    file_bytes: Optional[bytes] = None,
    mime: str,
    filename_original: Optional[str] = None,
    data_emissione: Optional[str] = None,
    descrizione: Optional[str] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Upload singolo documento end-to-end (prepare → R2 PUT → commit).

    Args:
        client: IngestClient autenticato
        external_id_pratica: external_id della pratica
        external_document_id: identificativo univoco del documento nel sistema cliente
        tipo_documento: valore da enum TipoDocumento (es. "contratto_originario")
        file_path: path file (mutuamente esclusivo con file_bytes)
        file_bytes: bytes già in memoria
        mime: content-type MIME (es. "application/pdf")
        filename_original, data_emissione, descrizione: metadata opzionali
        extra_metadata: altri campi DocumentoDict

    Returns:
        Dict con document_id, state, sha256_verified.
    """
    if (file_path is None) == (file_bytes is None):
        raise ValueError("fornisci esattamente uno tra file_path e file_bytes")

    if file_bytes is None:
        file_bytes = Path(file_path).read_bytes()

    sha = _sha256_bytes(file_bytes)
    size = len(file_bytes)

    doc_meta = {
        "external_id": external_document_id,
        "tipo_documento": tipo_documento,
        "mime": mime,
        "size_bytes": size,
        "sha256_expected": sha,
        "pratica_external_id": external_id_pratica,
    }
    if filename_original:
        doc_meta["filename_original"] = filename_original
    if data_emissione:
        doc_meta["data_emissione"] = data_emissione
    if descrizione:
        doc_meta["descrizione"] = descrizione
    if extra_metadata:
        doc_meta.update(extra_metadata)

    # Step 1: prepare
    uploads = client.documents_prepare(external_id_pratica, [doc_meta])
    if not uploads:
        raise IngestError("documents:prepare returned empty uploads")
    presigned = uploads[0]

    # Step 2: PUT su R2
    client.put_to_r2(
        presigned["presigned_put_url"],
        file_bytes=file_bytes,
        content_type=mime,
    )

    # Step 3: commit per verify + AV
    commit_resp = client.documents_commit(
        external_id_pratica,
        presigned["document_id"],
        client_sha256=sha,
    )
    return {
        **commit_resp,
        "document_id": presigned["document_id"],
        "external_id": external_document_id,
        "r2_key": presigned.get("r2_key"),
        "sha256": sha,
        "size_bytes": size,
    }


def upload_documents_batch(
    client: IngestClient,
    *,
    external_id_pratica: str,
    documents: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Upload multiplo documenti per una pratica (prepare unico + N PUT + N commit).

    Args:
        documents: lista dict ognuno con:
            - external_document_id, tipo_documento, mime
            - (file_path OR file_bytes)
            - opzionali: filename_original, data_emissione, descrizione, extra_metadata

    Ritorna lista di risultati per singolo documento.
    """
    # Prepare batch: costruisci metadata per tutti
    prepare_payload = []
    file_data_list = []  # per ogni doc: (bytes, sha, mime)
    for d in documents:
        if (d.get("file_path") is None) == (d.get("file_bytes") is None):
            raise ValueError(f"documento {d.get('external_document_id')}: usa file_path XOR file_bytes")
        file_bytes = d.get("file_bytes") or Path(d["file_path"]).read_bytes()
        sha = _sha256_bytes(file_bytes)
        size = len(file_bytes)
        meta = {
            "external_id": d["external_document_id"],
            "tipo_documento": d["tipo_documento"],
            "mime": d["mime"],
            "size_bytes": size,
            "sha256_expected": sha,
            "pratica_external_id": external_id_pratica,
        }
        for k in ("filename_original", "data_emissione", "descrizione"):
            if d.get(k):
                meta[k] = d[k]
        if d.get("extra_metadata"):
            meta.update(d["extra_metadata"])
        prepare_payload.append(meta)
        file_data_list.append((file_bytes, sha, d["mime"]))

    uploads = client.documents_prepare(external_id_pratica, prepare_payload)
    if len(uploads) != len(documents):
        raise IngestError(f"prepare returned {len(uploads)} URLs for {len(documents)} docs")

    results = []
    for i, (presigned, (file_bytes, sha, mime)) in enumerate(zip(uploads, file_data_list)):
        try:
            client.put_to_r2(presigned["presigned_put_url"], file_bytes, content_type=mime)
            commit_resp = client.documents_commit(
                external_id_pratica, presigned["document_id"], client_sha256=sha,
            )
            results.append({**commit_resp, "document_id": presigned["document_id"], "sha256": sha, "ok": True})
        except Exception as exc:
            results.append({
                "document_id": presigned["document_id"],
                "external_id": presigned["external_id"],
                "ok": False,
                "error": str(exc),
            })

    return results
