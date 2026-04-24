"""Test documents helper."""
import tempfile
from unittest.mock import MagicMock

import pytest


def test_upload_document_from_bytes():
    from credilex_ingestion.documents import upload_document

    client = MagicMock()
    client.documents_prepare.return_value = [{
        "document_id": "doc-1",
        "external_id": "D1",
        "presigned_put_url": "https://r2.example/put?sig=...",
        "r2_key": "archive/x/y/D1.pdf",
    }]
    client.documents_commit.return_value = {"state": "verifying", "document_id": "doc-1"}

    result = upload_document(
        client,
        external_id_pratica="P1",
        external_document_id="D1",
        tipo_documento="contratto_originario",
        file_bytes=b"PDF-content",
        mime="application/pdf",
    )

    assert result["document_id"] == "doc-1"
    assert client.documents_prepare.called
    assert client.put_to_r2.called
    assert client.documents_commit.called
    # Verifica sha256
    import hashlib
    assert result["sha256"] == hashlib.sha256(b"PDF-content").hexdigest()


def test_upload_document_requires_one_source():
    from credilex_ingestion.documents import upload_document
    client = MagicMock()
    with pytest.raises(ValueError):
        upload_document(
            client, external_id_pratica="P1", external_document_id="D1",
            tipo_documento="x", mime="application/pdf",
        )  # nessun file_path ne' file_bytes
    with pytest.raises(ValueError):
        upload_document(
            client, external_id_pratica="P1", external_document_id="D1",
            tipo_documento="x", mime="application/pdf",
            file_path="/x", file_bytes=b"y",  # entrambi
        )


def test_upload_documents_batch():
    from credilex_ingestion.documents import upload_documents_batch
    client = MagicMock()
    client.documents_prepare.return_value = [
        {"document_id": "d1", "external_id": "D1", "presigned_put_url": "https://r2.test/1", "r2_key": "k1"},
        {"document_id": "d2", "external_id": "D2", "presigned_put_url": "https://r2.test/2", "r2_key": "k2"},
    ]
    client.documents_commit.return_value = {"state": "verifying"}

    docs = [
        {"external_document_id": "D1", "tipo_documento": "x", "mime": "application/pdf", "file_bytes": b"aa"},
        {"external_document_id": "D2", "tipo_documento": "x", "mime": "application/pdf", "file_bytes": b"bb"},
    ]
    results = upload_documents_batch(client, external_id_pratica="P1", documents=docs)
    assert len(results) == 2
    assert all(r["ok"] for r in results)
