"""Integration-ish tests per IngestClient - HTTP mockato con pytest-httpx."""
import json
import pytest

try:
    from pytest_httpx import HTTPXMock
    _HAS_PYTEST_HTTPX = True
except ImportError:
    _HAS_PYTEST_HTTPX = False


pytestmark = pytest.mark.skipif(not _HAS_PYTEST_HTTPX, reason="pytest-httpx not installed")


@pytest.fixture
def client():
    from credilex_ingestion import IngestClient
    return IngestClient(
        api_id="clx_id_test_abc",
        api_secret="clx_sk_test_secret",
        base_url="https://ingest.test.example",
        max_retries=1,  # riduci per test veloci
    )


def test_health(client, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url="https://ingest.test.example/health",
        json={"status": "ok", "service": "ingestion-service"},
    )
    res = client.health()
    assert res["status"] == "ok"


def test_validate_pratica(client, httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:validate",
        json={"external_id": "X1", "valid": True, "violations": [], "warnings": []},
    )
    res = client.validate_pratica({"pratica": {"external_id": "X1"}, "soggetti": []})
    assert res["valid"] is True


def test_upload_batch_multistatus(client, httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=207,
        json={
            "session_id": "sess-1", "batch_size": 2,
            "total_accepted": 1, "total_rejected": 1,
            "accepted": [{"external_id": "X1", "staging_id": "s1", "state": "valid"}],
            "rejected": [{"external_id": "X2", "batch_index": 1, "violations": [{"code": "business.npl.missing_data_classificazione"}]}],
            "request_id": "req-1",
        },
    )
    res = client.upload_batch(pratiche=[{"pratica": {}}, {"pratica": {}}])
    assert res["total_accepted"] == 1
    assert res["total_rejected"] == 1


def test_error_401_mapped_to_autherror(client, httpx_mock):
    from credilex_ingestion import AuthError
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=401,
        json={"error": {"code": "auth.credential.invalid", "message": "unauthorized"}},
    )
    with pytest.raises(AuthError):
        client.upload_batch(pratiche=[{"pratica": {}}])


def test_error_422_mapped_to_validation(client, httpx_mock):
    from credilex_ingestion import IngestValidationError
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=422,
        json={"error": {"code": "validation_failed"}, "violations": [{"code": "syntax.field.required", "path": "$.x"}]},
    )
    with pytest.raises(IngestValidationError) as exc_info:
        client.upload_batch(pratiche=[{"pratica": {}}])
    assert len(exc_info.value.violations) == 1


def test_retry_on_503(client, httpx_mock):
    # 3 chiamate: 2 * 503 poi 1 * 200
    httpx_mock.add_response(status_code=503, method="GET", url="https://ingest.test.example/health")
    httpx_mock.add_response(status_code=200, method="GET", url="https://ingest.test.example/health", json={"status": "ok"})
    # max_retries=1 -> totale 2 tentativi
    res = client.health()
    assert res["status"] == "ok"


# --- BUG #1: put_to_r2 streaming ------------------------------------------


def test_put_to_r2_streaming_from_path(client, httpx_mock, tmp_path):
    """Upload streaming da file_path — httpx riceve un file handle, non bytes."""
    file_ = tmp_path / "large.bin"
    file_.write_bytes(b"X" * 10_000)

    httpx_mock.add_response(method="PUT", url="https://r2.test.example/put?sig=abc", status_code=200)

    client.put_to_r2(
        "https://r2.test.example/put?sig=abc",
        file_path=str(file_),
        content_type="application/octet-stream",
    )

    req = httpx_mock.get_requests()[0]
    assert req.headers["Content-Length"] == "10000"
    assert req.headers["Content-Type"] == "application/octet-stream"


def test_put_to_r2_streaming_from_fileobj(client, httpx_mock, tmp_path):
    import io
    buf = io.BytesIO(b"hello-world-streamed")

    httpx_mock.add_response(method="PUT", url="https://r2.test.example/put2", status_code=200)

    client.put_to_r2(
        "https://r2.test.example/put2",
        file_obj=buf,
        content_type="text/plain",
    )

    req = httpx_mock.get_requests()[0]
    assert req.headers["Content-Length"] == str(len(b"hello-world-streamed"))


def test_put_to_r2_bytes_backward_compat(client, httpx_mock):
    """Call positional con bytes continua a funzionare (backward compat)."""
    httpx_mock.add_response(method="PUT", url="https://r2.test.example/put3", status_code=200)
    client.put_to_r2("https://r2.test.example/put3", b"small-payload", content_type="application/pdf")
    req = httpx_mock.get_requests()[0]
    assert req.headers["Content-Length"] == "13"


def test_put_to_r2_requires_exactly_one_source(client):
    with pytest.raises(ValueError, match="esattamente uno"):
        client.put_to_r2("https://r2.test.example/put4")  # zero sources
    with pytest.raises(ValueError, match="esattamente uno"):
        client.put_to_r2(
            "https://r2.test.example/put4",
            file_bytes=b"x", file_path="/tmp/y",
        )


def test_put_to_r2_error_raises_ingesterror(client, httpx_mock):
    from credilex_ingestion import IngestError
    httpx_mock.add_response(method="PUT", url="https://r2.test.example/put5", status_code=403, text="forbidden")
    with pytest.raises(IngestError, match="R2 upload failed"):
        client.put_to_r2("https://r2.test.example/put5", b"x")


# --- BUG #3: error mapping ------------------------------------------------


def test_error_402_mapped_to_quota(client, httpx_mock):
    from credilex_ingestion import QuotaError
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=402,
        json={"error": {"code": "quota.payment_required", "message": "payment required"}},
    )
    with pytest.raises(QuotaError) as exc_info:
        client.upload_batch(pratiche=[{"pratica": {}}])
    assert exc_info.value.code == "quota.payment_required"


def test_error_409_mapped_to_validation_error(client, httpx_mock):
    from credilex_ingestion import IngestValidationError
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=409,
        json={"error": {"message": "idempotency conflict"}},
    )
    with pytest.raises(IngestValidationError) as exc_info:
        client.upload_batch(pratiche=[{"pratica": {}}])
    assert exc_info.value.code == "integrity.conflict"


def test_error_413_mapped_to_quota(client, httpx_mock):
    from credilex_ingestion import QuotaError
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=413,
        json={"error": {"code": "payload_too_large", "message": "too large"}},
    )
    with pytest.raises(QuotaError):
        client.upload_batch(pratiche=[{"pratica": {}}])


def test_error_415_mapped_to_ingest_error(client, httpx_mock):
    from credilex_ingestion import IngestError
    httpx_mock.add_response(
        method="POST",
        url="https://ingest.test.example/ingest/v1/pratiche:batch",
        status_code=415,
        json={"error": {"message": "unsupported media type"}},
    )
    with pytest.raises(IngestError) as exc_info:
        client.upload_batch(pratiche=[{"pratica": {}}])
    assert exc_info.value.code == "syntax.content_type_invalid"


# --- BUG #4: async upload_batch signature parity --------------------------


def test_async_upload_batch_signature_parity():
    """AsyncIngestClient.upload_batch deve accettare gli stessi kwargs espliciti del sync."""
    import inspect

    from credilex_ingestion import AsyncIngestClient, IngestClient

    sync_sig = inspect.signature(IngestClient.upload_batch)
    async_sig = inspect.signature(AsyncIngestClient.upload_batch)

    expected = {"pratiche", "batch_index", "batch_total", "batch_sha256", "idempotency_key"}
    sync_params = set(sync_sig.parameters) - {"self"}
    async_params = set(async_sig.parameters) - {"self"}

    assert expected.issubset(sync_params), f"sync missing: {expected - sync_params}"
    assert expected.issubset(async_params), f"async missing: {expected - async_params}"
    # Async non deve più avere **kwargs generico
    assert "kwargs" not in async_sig.parameters
