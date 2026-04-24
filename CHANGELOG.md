# Changelog

All notable changes documented here. Follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.1.0] - 2026-04-24

### Added
- Initial release
- `IngestClient` sync client with HMAC signing
- `AsyncIngestClient` async client (httpx)
- `upload_batch()`, `validate_pratica()`, `documents_prepare()`, `documents_commit()`
- Automatic retry with exponential backoff
- `BatchSplitter` helper for large portfolios
- Full test suite with pytest-httpx
