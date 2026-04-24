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
