"""Batch splitter helper for large portfolio uploads.

Uso:
    from credilex_ingestion import IngestClient, BatchSplitter

    client = IngestClient(api_id=..., api_secret=...)
    splitter = BatchSplitter(client, batch_size=500)

    # Sync sequential
    results = splitter.upload_all(pratiche=[...], show_progress=True)
    print(f"Batches sent: {results.total_batches}")
    print(f"Accepted: {results.total_accepted}, Rejected: {results.total_rejected}")

    # Con callback per rejected (retry/fix)
    def on_rejected(batch_idx, rejected_list):
        for r in rejected_list:
            print(f"Batch {batch_idx} rejected {r['external_id']}: {r['violations']}")
    results = splitter.upload_all(pratiche=[...], on_rejected=on_rejected)
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

from credilex_ingestion.client import IngestClient
from credilex_ingestion.errors import IngestError

DEFAULT_BATCH_SIZE = 500


@dataclass
class BatchUploadResult:
    """Aggregazione risultati di un upload multi-batch."""
    total_batches: int = 0
    total_pratiche: int = 0
    total_accepted: int = 0
    total_rejected: int = 0
    batch_responses: List[Dict[str, Any]] = field(default_factory=list)
    rejected_per_batch: List[List[Dict[str, Any]]] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    duration_seconds: float = 0.0


class BatchSplitter:
    """Divide una lista grande di pratiche in batch e li uploada sequenzialmente."""

    def __init__(
        self,
        client: IngestClient,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        generate_idempotency_keys: bool = True,
    ):
        if batch_size < 1 or batch_size > 500:
            raise ValueError(f"batch_size deve essere tra 1 e 500, ricevuto {batch_size}")
        self.client = client
        self.batch_size = batch_size
        self.generate_idempotency_keys = generate_idempotency_keys

    @staticmethod
    def chunk(pratiche: List[dict], size: int) -> Iterable[List[dict]]:
        """Yield chunks di dimensione fissa (l'ultimo può essere più piccolo)."""
        for i in range(0, len(pratiche), size):
            yield pratiche[i : i + size]

    def upload_all(
        self,
        *,
        pratiche: List[dict],
        on_accepted: Optional[Callable[[int, List[dict]], None]] = None,
        on_rejected: Optional[Callable[[int, List[dict]], None]] = None,
        on_error: Optional[Callable[[int, Exception], None]] = None,
        show_progress: bool = False,
    ) -> BatchUploadResult:
        """Upload sequenziale di tutte le pratiche.

        Args:
            pratiche: lista di PraticaPayload dict
            on_accepted(batch_idx, accepted_list): callback per ogni batch con pratiche accettate
            on_rejected(batch_idx, rejected_list): callback per rigettate
            on_error(batch_idx, exception): se intero batch fallisce (network/server)
            show_progress: stampa progress su stdout

        Returns:
            BatchUploadResult con aggregati + batch_responses grezze.
        """
        result = BatchUploadResult()
        start = time.monotonic()
        batches = list(self.chunk(pratiche, self.batch_size))
        total_batches = len(batches)

        for idx, batch in enumerate(batches):
            if show_progress:
                print(f"[BatchSplitter] Batch {idx+1}/{total_batches} ({len(batch)} pratiche)...", flush=True)
            idemp = None
            if self.generate_idempotency_keys:
                idemp = f"batch_{idx}_{uuid.uuid4().hex[:16]}"
            try:
                resp = self.client.upload_batch(
                    pratiche=batch,
                    batch_index=idx,
                    batch_total=total_batches,
                    idempotency_key=idemp,
                )
                result.batch_responses.append(resp)
                accepted = resp.get("accepted", []) or []
                rejected = resp.get("rejected", []) or []
                result.total_accepted += len(accepted)
                result.total_rejected += len(rejected)
                result.rejected_per_batch.append(rejected)

                if on_accepted and accepted:
                    on_accepted(idx, accepted)
                if on_rejected and rejected:
                    on_rejected(idx, rejected)

                if show_progress:
                    print(f"   accepted={len(accepted)} rejected={len(rejected)}", flush=True)

            except Exception as exc:
                result.errors.append({"batch_index": idx, "error": str(exc)})
                if on_error:
                    on_error(idx, exc)
                if show_progress:
                    print(f"   ERROR: {exc}", flush=True)

        result.total_batches = total_batches
        result.total_pratiche = len(pratiche)
        result.duration_seconds = time.monotonic() - start
        return result

    def retry_rejected(
        self,
        *,
        rejected_list: List[Dict[str, Any]],
        corrected_pratiche: List[dict],
    ) -> Dict[str, Any]:
        """Helper per re-upload solo delle pratiche corrette.

        Il cliente, ricevuta una lista di `rejected`, corregge i dati e chiama
        questo metodo passando SOLO le pratiche corrette. Le external_id devono
        matchare le precedenti (upsert server-side).
        """
        if not corrected_pratiche:
            return {"total_accepted": 0, "total_rejected": 0, "skipped": True}
        return self.client.upload_batch(
            pratiche=corrected_pratiche,
            idempotency_key=f"retry_{uuid.uuid4().hex[:16]}",
        )
