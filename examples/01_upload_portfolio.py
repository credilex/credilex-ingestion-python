"""Esempio: upload di un portafoglio base di pratiche NPL.

Uso:
    export CREDILEX_API_ID=clx_id_live_xxxxx
    export CREDILEX_API_SECRET=clx_sk_live_yyyyy
    python examples/01_upload_portfolio.py
"""
import json
import os
import uuid

from credilex_ingestion import IngestClient, BatchSplitter


def main():
    api_id = os.environ["CREDILEX_API_ID"]
    api_secret = os.environ["CREDILEX_API_SECRET"]
    base_url = os.environ.get("CREDILEX_BASE_URL", "https://ingest.credilex.it")

    # Costruisci un batch di esempio (in produzione leggerai da DB/CSV/sistema legacy)
    pratiche = []
    for i in range(1, 6):  # 5 pratiche di esempio
        pratiche.append({
            "pratica": {
                "external_id": f"ACME-DEMO-{i:04d}",
                "tipo_portafoglio": "npl",
                "prodotto": "mutuo_ipotecario",
                "istituto_cedente": "Banca Esempio SpA",
                "valuta": "EUR",
                "importo_capitale_residuo": "145678.50",
                "importo_interessi_maturati": "8240.30",
                "importo_interessi_mora": "3120.00",
                "spese_legali_sostenute": "1250.00",
                "spese_varie": "389.20",
                "importo_totale": "158678.00",
                "data_classificazione_sofferenza": "2023-02-01",
            },
            "soggetti": [],
            "garanzie": [],
            "immobili": [],
            "documenti": [],
        })

    with IngestClient(api_id=api_id, api_secret=api_secret, base_url=base_url) as client:
        # 1. Health check
        print("Health:", client.health())

        # 2. Dry-run validate (senza auth)
        sample = pratiche[0]
        val = client.validate_pratica(sample)
        print(f"validate: valid={val.get('valid')} violations={len(val.get('violations', []))}")

        # 3. Upload batch con splitter (auto-split se >500 pratiche)
        splitter = BatchSplitter(client, batch_size=500)

        def on_rejected(batch_idx, rejected):
            print(f"Batch {batch_idx}: {len(rejected)} pratiche rigettate")
            for r in rejected[:3]:
                print(f"   - {r['external_id']}: {[v['code'] for v in r.get('violations', [])]}")

        result = splitter.upload_all(
            pratiche=pratiche,
            on_rejected=on_rejected,
            show_progress=True,
        )

        print(f"\nRisultati:")
        print(f"   batches: {result.total_batches}")
        print(f"   totale: {result.total_pratiche}")
        print(f"   accettate: {result.total_accepted}")
        print(f"   rigettate: {result.total_rejected}")
        print(f"   durata: {result.duration_seconds:.2f}s")

        # 4. Summary stato sessione
        summary = client.get_summary()
        print(f"\nSession state: {summary.get('state')}")


if __name__ == "__main__":
    main()
