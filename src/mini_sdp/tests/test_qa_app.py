"""Tests for the QA web app: listing, approve/reprocess writes, and FITS preview."""
from pathlib import Path

import numpy as np
import pytest
from astropy.io import fits
from fastapi.testclient import TestClient

from mini_sdp.data.models import (
    DatasetStatus,
    ObservingBlock,
    ProcessedDataset,
    ProcessingBlock,
    QAAssessment,
    QAStatus,
)
from mini_sdp.data.store import StateStore
from mini_sdp.qa.app import create_app


@pytest.fixture
def store(tmp_path) -> StateStore:
    s = StateStore(tmp_path / "state.db")
    s.init_schema()
    yield s
    s.close()


def seed_pending(store, dataset_dir: Path):
    obs = store.insert_observing_block(ObservingBlock())
    pd = store.insert_processed_dataset(
        ProcessedDataset(path=dataset_dir, size=500, status=DatasetStatus.STORED)
    )
    store.insert_processing_block(
        ProcessingBlock(observing_block_id=obs.id, processed_dataset_id=pd.id)
    )
    qa = store.insert_qa_assessment(QAAssessment(processed_dataset_id=pd.id))
    return qa, pd


def test_list_pending(store, tmp_path):
    qa, pd = seed_pending(store, tmp_path / "run")
    client = TestClient(create_app(store))
    data = client.get("/api/datasets").json()
    assert len(data) == 1
    assert data[0]["qa_id"] == qa.id
    assert data[0]["dataset_id"] == pd.id


def test_approve_and_reprocess_write_db(store, tmp_path):
    qa, _ = seed_pending(store, tmp_path / "run")
    client = TestClient(create_app(store))

    assert client.post(f"/api/qa/{qa.id}/approve").status_code == 200
    assert store.get_qa_assessment(qa.id).status is QAStatus.APPROVED

    assert client.post(f"/api/qa/{qa.id}/reprocess").status_code == 200
    assert store.get_qa_assessment(qa.id).status is QAStatus.REJECTED


def test_approve_unknown_returns_404(store):
    client = TestClient(create_app(store))
    assert client.post("/api/qa/999/approve").status_code == 404


def test_preview_renders_real_fits(store, tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    fits.writeto(run_dir / "out-image.fits", np.random.rand(16, 16).astype("float32"))
    _, pd = seed_pending(store, run_dir)

    client = TestClient(create_app(store))
    resp = client.get(f"/api/datasets/{pd.id}/preview.png")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/png"
    assert resp.content[:8] == b"\x89PNG\r\n\x1a\n"


def test_preview_placeholder_when_no_fits(store, tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _, pd = seed_pending(store, run_dir)

    client = TestClient(create_app(store))
    resp = client.get(f"/api/datasets/{pd.id}/preview.png")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/png"
