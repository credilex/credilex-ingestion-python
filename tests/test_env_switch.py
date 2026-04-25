"""Test env switch (production/sandbox) + X-Credilex-Env response check."""
import pytest

try:
    from pytest_httpx import HTTPXMock  # noqa: F401
    _HAS_PYTEST_HTTPX = True
except ImportError:
    _HAS_PYTEST_HTTPX = False


pytestmark = pytest.mark.skipif(not _HAS_PYTEST_HTTPX, reason="pytest-httpx not installed")


# --- Constructor / base_url resolution -------------------------------------


def test_default_env_is_production():
    """Senza specificare env, default = production con URL prod."""
    from credilex_ingestion import IngestClient
    c = IngestClient(api_id="clx_id_x", api_secret="clx_sk_y")
    assert c.env == "production"
    assert c.base_url == "https://ingest.credilex.it"


def test_env_production_explicit():
    from credilex_ingestion import IngestClient
    c = IngestClient(api_id="clx_id_x", api_secret="clx_sk_y", env="production")
    assert c.env == "production"
    assert c.base_url == "https://ingest.credilex.it"


def test_env_sandbox_uses_sandbox_url():
    from credilex_ingestion import IngestClient
    c = IngestClient(api_id="clx_id_x", api_secret="clx_sk_y", env="sandbox")
    assert c.env == "sandbox"
    assert c.base_url == "https://sandbox.credilex.it"


def test_base_url_override_wins_over_env():
    """base_url= esplicito vince anche con env=sandbox (custom enterprise endpoint)."""
    from credilex_ingestion import IngestClient
    c = IngestClient(
        api_id="clx_id_x",
        api_secret="clx_sk_y",
        env="sandbox",
        base_url="https://custom.example.com",
    )
    assert c.env == "sandbox"  # env is preserved as label
    assert c.base_url == "https://custom.example.com"


def test_env_invalid_raises_valueerror():
    from credilex_ingestion import IngestClient
    with pytest.raises(ValueError, match="env must be 'production' or 'sandbox'"):
        IngestClient(api_id="x", api_secret="y", env="staging")


def test_class_constants_exposed():
    from credilex_ingestion import IngestClient
    assert IngestClient.PROD_BASE_URL == "https://ingest.credilex.it"
    assert IngestClient.SANDBOX_BASE_URL == "https://sandbox.credilex.it"


def test_async_client_env_sandbox():
    from credilex_ingestion import AsyncIngestClient
    c = AsyncIngestClient(api_id="x", api_secret="y", env="sandbox")
    assert c.env == "sandbox"
    assert c.base_url == "https://sandbox.credilex.it"


def test_async_client_env_invalid():
    from credilex_ingestion import AsyncIngestClient
    with pytest.raises(ValueError, match="env must be"):
        AsyncIngestClient(api_id="x", api_secret="y", env="dev")


# --- Header verification on response ---------------------------------------


@pytest.fixture
def sandbox_client():
    from credilex_ingestion import IngestClient
    return IngestClient(
        api_id="clx_id_test",
        api_secret="clx_sk_test",
        env="sandbox",
        max_retries=0,
    )


@pytest.fixture
def prod_client():
    from credilex_ingestion import IngestClient
    return IngestClient(
        api_id="clx_id_test",
        api_secret="clx_sk_test",
        # default env=production -> base_url=https://ingest.credilex.it
        max_retries=0,
    )


def test_sandbox_client_accepts_matching_header(sandbox_client, httpx_mock):
    """Server risponde X-Credilex-Env: sandbox e client è sandbox -> OK."""
    httpx_mock.add_response(
        method="GET",
        url="https://sandbox.credilex.it/health",
        json={"status": "ok"},
        headers={"X-Credilex-Env": "sandbox"},
    )
    res = sandbox_client.health()
    assert res["status"] == "ok"


def test_sandbox_client_rejects_prod_header(sandbox_client, httpx_mock):
    """Client sandbox + server X-Credilex-Env: prod -> IngestConfigError."""
    from credilex_ingestion import IngestConfigError

    httpx_mock.add_response(
        method="GET",
        url="https://sandbox.credilex.it/health",
        json={"status": "ok"},
        headers={"X-Credilex-Env": "prod"},
    )
    with pytest.raises(IngestConfigError) as exc_info:
        sandbox_client.health()
    assert "expected 'sandbox'" in str(exc_info.value)
    assert "prod" in str(exc_info.value)
    assert exc_info.value.code == "config.env_mismatch"


def test_prod_client_accepts_matching_header(prod_client, httpx_mock):
    """Client production + server X-Credilex-Env: prod -> OK."""
    httpx_mock.add_response(
        method="GET",
        url="https://ingest.credilex.it/health",
        json={"status": "ok"},
        headers={"X-Credilex-Env": "prod"},
    )
    res = prod_client.health()
    assert res["status"] == "ok"


def test_prod_client_rejects_sandbox_header(prod_client, httpx_mock):
    """Client production + server X-Credilex-Env: sandbox -> IngestConfigError."""
    from credilex_ingestion import IngestConfigError

    httpx_mock.add_response(
        method="GET",
        url="https://ingest.credilex.it/health",
        json={"status": "ok"},
        headers={"X-Credilex-Env": "sandbox"},
    )
    with pytest.raises(IngestConfigError):
        prod_client.health()


def test_missing_header_does_not_raise(sandbox_client, httpx_mock):
    """Server vecchio senza header -> nessun raise (backward-compat)."""
    httpx_mock.add_response(
        method="GET",
        url="https://sandbox.credilex.it/health",
        json={"status": "ok"},
    )
    res = sandbox_client.health()
    assert res["status"] == "ok"


def test_4xx_response_skips_env_check(sandbox_client, httpx_mock):
    """Su 4xx il check è skipped (es. 401 da nginx senza header) -> errore HTTP normale."""
    from credilex_ingestion import AuthError

    httpx_mock.add_response(
        method="POST",
        url="https://sandbox.credilex.it/ingest/v1/pratiche:batch",
        status_code=401,
        # NON mettiamo X-Credilex-Env: prod — simuliamo nginx che blocca prima dell'app
        json={"error": {"code": "auth.credential.invalid", "message": "unauthorized"}},
    )
    with pytest.raises(AuthError):
        sandbox_client.upload_batch(pratiche=[{"pratica": {}}])


def test_header_case_insensitive(sandbox_client, httpx_mock):
    """Header value in maiuscolo/altro case viene normalizzato."""
    httpx_mock.add_response(
        method="GET",
        url="https://sandbox.credilex.it/health",
        json={"status": "ok"},
        headers={"X-Credilex-Env": "Sandbox"},
    )
    # case-insensitive match -> pass
    res = sandbox_client.health()
    assert res["status"] == "ok"


def test_async_client_env_mismatch_raises(httpx_mock):
    """Async client con env=sandbox e server prod -> IngestConfigError."""
    import asyncio

    from credilex_ingestion import AsyncIngestClient, IngestConfigError

    httpx_mock.add_response(
        method="GET",
        url="https://sandbox.credilex.it/health",
        json={"status": "ok"},
        headers={"X-Credilex-Env": "prod"},
    )

    async def _run():
        c = AsyncIngestClient(api_id="x", api_secret="y", env="sandbox", max_retries=0)
        try:
            with pytest.raises(IngestConfigError):
                await c.health()
        finally:
            await c.aclose()

    asyncio.run(_run())
