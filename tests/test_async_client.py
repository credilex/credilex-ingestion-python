"""Test AsyncIngestClient — HTTP mockato con pytest-httpx."""
import pytest

try:
    from pytest_httpx import HTTPXMock  # noqa: F401
    _HAS_PYTEST_HTTPX = True
except ImportError:
    _HAS_PYTEST_HTTPX = False


pytestmark = pytest.mark.skipif(not _HAS_PYTEST_HTTPX, reason="pytest-httpx not installed")


@pytest.fixture
def async_client():
    from credilex_ingestion import AsyncIngestClient
    return AsyncIngestClient(
        api_id="clx_id_test_abc",
        api_secret="clx_sk_test_secret",
        base_url="https://ingest.test.example",
        max_retries=1,
    )


async def test_async_health(async_client, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url="https://ingest.test.example/health",
        json={"status": "ok", "service": "ingestion-service"},
    )
    res = await async_client.health()
    assert res["status"] == "ok"
    await async_client.aclose()


async def test_async_upload_batch(async_client, httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=207,
        json={
            "session_id": "sess-1", "batch_size": 2,
            "total_accepted": 2, "total_rejected": 0,
            "accepted": [
                {"external_id": "X1", "staging_id": "s1", "state": "valid"},
                {"external_id": "X2", "staging_id": "s2", "state": "valid"},
            ],
            "rejected": [],
            "request_id": "req-1",
        },
    )
    res = await async_client.upload_batch(
        pratiche=[{"pratica": {}}, {"pratica": {}}],
        batch_index=0,
        batch_total=1,
        batch_sha256="a" * 64,
        idempotency_key="k1",
    )
    assert res["total_accepted"] == 2
    # Verifica che Idempotency-Key e body siano stati inoltrati correttamente
    req = httpx_mock.get_requests()[0]
    assert req.headers.get("Idempotency-Key") == "k1"
    import json
    body = json.loads(req.content)
    assert body["batch_index"] == 0
    assert body["batch_total"] == 1
    assert body["batch_sha256"] == "a" * 64
    await async_client.aclose()


async def test_async_put_to_r2_streaming_from_path(async_client, httpx_mock, tmp_path):
    file_ = tmp_path / "big.bin"
    file_.write_bytes(b"Y" * 5_000)

    httpx_mock.add_response(method="PUT", url="https://r2.test.example/async-put", status_code=200)

    await async_client.put_to_r2(
        "https://r2.test.example/async-put",
        file_path=str(file_),
        content_type="application/octet-stream",
    )

    req = httpx_mock.get_requests()[0]
    assert req.headers["Content-Length"] == "5000"
    await async_client.aclose()


async def test_async_put_to_r2_requires_exactly_one_source(async_client):
    with pytest.raises(ValueError, match="esattamente uno"):
        await async_client.put_to_r2("https://r2.test.example/x")
    await async_client.aclose()
