"""Test BatchSplitter."""
import pytest
from unittest.mock import MagicMock

from credilex_ingestion import BatchSplitter
from credilex_ingestion.batch import BatchUploadResult


def test_chunk():
    data = list(range(1005))
    chunks = list(BatchSplitter.chunk(data, 500))
    assert len(chunks) == 3
    assert len(chunks[0]) == 500
    assert len(chunks[-1]) == 5


def test_invalid_batch_size():
    from credilex_ingestion.client import IngestClient
    client = MagicMock(spec=IngestClient)
    with pytest.raises(ValueError):
        BatchSplitter(client, batch_size=0)
    with pytest.raises(ValueError):
        BatchSplitter(client, batch_size=501)


def test_upload_all_aggregates():
    client = MagicMock()
    client.upload_batch.return_value = {
        "session_id": "s1", "total_accepted": 2, "total_rejected": 1,
        "accepted": [{"external_id": "a"}, {"external_id": "b"}],
        "rejected": [{"external_id": "c", "violations": [{"code": "semantic.x"}]}],
    }
    splitter = BatchSplitter(client, batch_size=3)

    pratiche = [{"pratica": {"external_id": f"p{i}"}} for i in range(7)]  # 7 pratiche -> 3 batches
    result = splitter.upload_all(pratiche=pratiche)

    assert result.total_batches == 3  # ceil(7/3)
    assert result.total_pratiche == 7
    assert result.total_accepted == 6  # 2*3 batches
    assert result.total_rejected == 3
    assert client.upload_batch.call_count == 3


def test_retry_rejected_via_client():
    client = MagicMock()
    client.upload_batch.return_value = {"total_accepted": 1, "total_rejected": 0}
    splitter = BatchSplitter(client)
    res = splitter.retry_rejected(rejected_list=[], corrected_pratiche=[{"pratica": {}}])
    assert res["total_accepted"] == 1
