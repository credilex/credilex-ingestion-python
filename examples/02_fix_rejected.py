"""Esempio: gestione di pratiche rigettate con correzione + reinvio.

Flusso:
1. Upload batch iniziale
2. Raccogli lista rigettate con code = 'semantic.quadratura.importo'
3. Correggi automaticamente (ricalcola importo_totale = somma componenti)
4. Re-upload solo delle corrette
"""
import os
from decimal import Decimal

from credilex_ingestion import IngestClient


def fix_quadratura(pratica_payload: dict) -> dict:
    """Correzione automatica quadratura: importo_totale = somma componenti."""
    p = pratica_payload["pratica"]
    total = (
        Decimal(p.get("importo_capitale_residuo", "0"))
        + Decimal(p.get("importo_interessi_maturati", "0"))
        + Decimal(p.get("importo_interessi_mora", "0"))
        + Decimal(p.get("spese_legali_sostenute", "0"))
        + Decimal(p.get("spese_varie", "0"))
    )
    p["importo_totale"] = str(total)
    return pratica_payload


def main():
    api_id = os.environ["CREDILEX_API_ID"]
    api_secret = os.environ["CREDILEX_API_SECRET"]

    # Pratica con quadratura errata (intenzionale)
    bad_pratica = {
        "pratica": {
            "external_id": "ACME-FIX-DEMO-0001",
            "tipo_portafoglio": "npl",
            "prodotto": "mutuo_ipotecario",
            "istituto_cedente": "Banca Esempio SpA",
            "valuta": "EUR",
            "importo_capitale_residuo": "100000.00",
            "importo_interessi_maturati": "5000.00",
            "importo_interessi_mora": "0.00",
            "spese_legali_sostenute": "0.00",
            "spese_varie": "0.00",
            "importo_totale": "99000.00",  # errato - dovrebbe essere 105000
            "data_classificazione_sofferenza": "2023-02-01",
        },
        "soggetti": [], "garanzie": [], "immobili": [], "documenti": [],
    }

    with IngestClient(api_id=api_id, api_secret=api_secret) as client:
        # Validate dry-run
        val = client.validate_pratica(bad_pratica)
        if not val.get("valid"):
            print(f"Pratica invalida: {len(val.get('violations', []))} violazioni")
            for v in val.get("violations", []):
                print(f"   - [{v.get('code')}] {v.get('message')}")

            # Auto-fix
            has_quadratura_err = any(
                v.get("code") == "semantic.quadratura.importo" for v in val.get("violations", [])
            )
            if has_quadratura_err:
                print("\nApplico auto-fix quadratura...")
                fixed = fix_quadratura(bad_pratica)
                val2 = client.validate_pratica(fixed)
                if val2.get("valid"):
                    print(f"Fix riuscito: importo_totale ora = {fixed['pratica']['importo_totale']}")
                    # Upload reale
                    resp = client.upload_batch(pratiche=[fixed])
                    print(f"Upload: accepted={resp['total_accepted']}")
                else:
                    print("Fix insufficiente, altre violazioni rimaste.")


if __name__ == "__main__":
    main()
