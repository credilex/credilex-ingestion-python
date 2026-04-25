# Changelog

All notable changes documented here. Follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.2.0] - 2026-04-25

### Added
- `env="production"` / `env="sandbox"` constructor parameter on `IngestClient` and
  `AsyncIngestClient` to switch between `https://ingest.credilex.it` (default) and
  `https://sandbox.credilex.it`
- Class constants `IngestClient.PROD_BASE_URL` and `IngestClient.SANDBOX_BASE_URL`
- `X-Credilex-Env` response header verification — raises new `IngestConfigError`
  when server environment does not match client `env` (only checked on 2xx
  responses with the header present, so old servers and 4xx-from-nginx still work)
- New exported exception `IngestConfigError`

### Changed
- `base_url=` is now optional in the constructor; if omitted it derives from `env=`.
  Explicit `base_url=` still wins (custom enterprise endpoints continue to work).

### Backward compatibility
- Calls without `env=` continue to target production — no breaking changes for
  existing v0.1.0 users.

## [0.1.0] - 2026-04-24

### Added
- Initial release
- `IngestClient` sync client with HMAC signing
- `AsyncIngestClient` async client (httpx)
- `upload_batch()`, `validate_pratica()`, `documents_prepare()`, `documents_commit()`
- Automatic retry with exponential backoff
- `BatchSplitter` helper for large portfolios
- Full test suite with pytest-httpx
