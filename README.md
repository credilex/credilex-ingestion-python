# credilex-ingestion-python

Official Python SDK for the Credilex Ingestion API.

Upload NPL/UTP/utility portfolios via the Credilex REST API from your own systems,
with HMAC-SHA256 authentication and retry/idempotency built-in.

[![PyPI](https://img.shields.io/pypi/v/credilex-ingestion.svg)](https://pypi.org/project/credilex-ingestion/)
[![Python](https://img.shields.io/pypi/pyversions/credilex-ingestion.svg)](https://pypi.org/project/credilex-ingestion/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Installation

```bash
pip install credilex-ingestion
```

Optional extras:

```bash
# Pydantic v2 model validation
pip install "credilex-ingestion[pydantic]"

# Development
pip install "credilex-ingestion[dev]"
```

## Environments

The SDK supports two environments out of the box. Switch with the `env=` parameter:

| Env          | Base URL                          | Use for                                  |
|--------------|-----------------------------------|------------------------------------------|
| `production` | `https://ingest.credilex.it`      | Live data (default)                      |
| `sandbox`    | `https://sandbox.credilex.it`     | Development & integration testing        |

```python
from credilex_ingestion import IngestClient

# Sandbox (development & integration testing)
client = IngestClient(api_id="...", api_secret="...", env="sandbox")

# Production (default — env="production" is implicit)
client = IngestClient(api_id="...", api_secret="...", env="production")

# Custom URL (per chi ha endpoint enterprise dedicato)
client = IngestClient(api_id="...", api_secret="...", base_url="https://custom.example.com")
```

Sandbox credentials are issued separately from production: an `api_id`/`api_secret`
pair is bound to a single environment and will not authenticate against the other.
The SDK verifies the `X-Credilex-Env` response header on each successful response
and raises `IngestConfigError` if the server's environment doesn't match the
client's configured `env` — typically a sign that `base_url` is pointing the wrong
way or production credentials are being used against sandbox (or vice versa).

## Quickstart

```python
from credilex_ingestion import IngestClient

client = IngestClient(
    api_id="clx_id_live_xxxxxxxxxxxxxxxx",
    api_secret="clx_sk_live_...",
    env="production",  # or "sandbox" for the test environment
)

# Dry-run validation
result = client.validate_pratica({"pratica": {...}, "soggetti": [], ...})
print(f"valid: {result['valid']}, violations: {len(result['violations'])}")

# Upload batch (up to 500 per call)
response = client.upload_batch(pratiche=[{...}, {...}, ...])
print(f"Accepted: {response['total_accepted']}, Rejected: {response['total_rejected']}")

# Upload documents for a pratica (presigned PUT R2)
upload_urls = client.documents_prepare(
    external_id_pratica="ACME-001",
    documents=[{"external_id": "DOC-1", "tipo_documento": "contratto_originario"}],
)
for u in upload_urls:
    with open("contratto.pdf", "rb") as f:
        client.put_to_r2(u["presigned_put_url"], file_bytes=f.read())
    client.documents_commit(external_id_pratica="ACME-001", document_id=u["document_id"])
```

### Async usage

```python
import asyncio
from credilex_ingestion import AsyncIngestClient

async def main():
    async with AsyncIngestClient(api_id="...", api_secret="...") as client:
        response = await client.upload_batch(pratiche=[...])
        print(response)

asyncio.run(main())
```

### Large portfolios — batch splitter

```python
from credilex_ingestion import IngestClient, BatchSplitter

client = IngestClient(api_id="...", api_secret="...")
all_pratiche = load_my_portfolio()  # e.g. 4500 records

for batch in BatchSplitter(all_pratiche, size=500):
    response = client.upload_batch(pratiche=batch)
    print(f"Batch: {response['total_accepted']} accepted")
```

## Features

- Full HMAC-SHA256 request signing (timestamp + nonce + content-sha256)
- Automatic retry on 5xx/429 with exponential backoff
- Idempotency-Key support for safe retries
- Batch splitter helper (split N pratiche into batches of 500)
- Direct R2 PUT for document upload (binary bypass server)
- Type-friendly (works with or without Pydantic v2)
- Python 3.8+

## Error handling

```python
from credilex_ingestion import (
    IngestClient,
    IngestError,
    IngestConfigError,
    AuthError,
    IngestValidationError,
    QuotaError,
)

client = IngestClient(api_id="...", api_secret="...")

try:
    client.upload_batch(pratiche=[...])
except IngestConfigError as e:
    print(f"Environment mismatch (wrong base_url/env?): {e}")
except AuthError as e:
    print(f"Credentials issue: {e}")
except IngestValidationError as e:
    for v in e.violations:
        print(f"- {v['path']}: {v['message']}")
except QuotaError as e:
    print(f"Rate limit hit: {e}")
except IngestError as e:
    print(f"Generic SDK error: {e.code} — {e}")
```

## Documentation

- API spec: https://ingest.credilex.it/docs
- Auth HMAC reference: https://ingest.credilex.it/docs#hmac
- Full integration playbook: https://ingest.credilex.it/docs#playbook
- Examples: [examples/](examples/)

## Support

Open an issue on GitHub for bugs or feature requests. For production support,
contact your Credilex integration manager.

## License

MIT — see [LICENSE](LICENSE).
