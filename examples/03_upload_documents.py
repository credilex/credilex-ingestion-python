"""Esempio: upload documenti per una pratica esistente.

Prerequisiti: la pratica 'ACME-DEMO-0001' deve esistere in staging (cioe' averla
uploadata in precedenza via :batch).
"""
import os

from credilex_ingestion import IngestClient
from credilex_ingestion.documents import upload_document, upload_documents_batch


def main():
    api_id = os.environ["CREDILEX_API_ID"]
    api_secret = os.environ["CREDILEX_API_SECRET"]
    external_id_pratica = "ACME-DEMO-0001"

    with IngestClient(api_id=api_id, api_secret=api_secret) as client:
        # Singolo documento
        if os.path.exists("/tmp/contratto.pdf"):
            result = upload_document(
                client,
                external_id_pratica=external_id_pratica,
                external_document_id="CONTRATTO-001",
                tipo_documento="contratto_originario",
                file_path="/tmp/contratto.pdf",
                mime="application/pdf",
                filename_original="contratto.pdf",
                data_emissione="2015-06-10",
            )
            print(f"Document uploaded: {result['document_id']} state={result['state']}")

        # Batch di documenti
        documents = [
            {
                "external_document_id": "DECR-001",
                "tipo_documento": "decreto_ingiuntivo",
                "file_bytes": b"stub-pdf-content",  # in produzione: open(path, 'rb').read()
                "mime": "application/pdf",
            },
            {
                "external_document_id": "PRECETTO-001",
                "tipo_documento": "precetto",
                "file_bytes": b"stub-pdf-content-2",
                "mime": "application/pdf",
            },
        ]
        results = upload_documents_batch(
            client,
            external_id_pratica=external_id_pratica,
            documents=documents,
        )
        for r in results:
            ok = r.get("ok", False)
            mark = "OK" if ok else "FAIL"
            print(f"   [{mark}] {r.get('external_id')}: {r.get('state', r.get('error'))}")

        # Verifica finale
        docs_list = client.list_documents(external_id_pratica)
        print(f"\nDocumenti totali per la pratica: {len(docs_list.get('documents', []))}")


if __name__ == "__main__":
    main()
