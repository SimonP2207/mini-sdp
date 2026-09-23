"""Quality Assessment web app (standalone FastAPI service).

A single-page UI plus a small REST API. It shares only the State Store with the
orchestrator: approving or re-processing a product just writes the QA row, which the
orchestrator picks up on its next reconcile pass. Processed images are rendered from FITS
to PNG for the reviewer.
"""
import io
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse

from ..data.models import QAStatus
from ..data.store import StateStore


def _find_image_fits(dataset_dir: Path) -> Path | None:
    if not dataset_dir.is_dir():
        return None
    matches = sorted(dataset_dir.glob("*-image.fits")) or sorted(dataset_dir.glob("*.fits"))
    return matches[0] if matches else None


def _render_fits_png(fits_path: Path) -> bytes:
    with fits.open(fits_path) as hdul:
        data = np.squeeze(hdul[0].data)
    while data.ndim > 2:
        data = data[0]
    
    std = np.nanstd(
        np.where(np.abs(data) < np.percentile(np.abs(data), 99.5), data, np.nan)
    )
    bunit = hdul[0].header.get('BUNIT')
    std_str = f"\u03c3 = {std:.2e} {bunit}"
    min_str = f"max = {np.min(data):.2e} {bunit}"
    max_str = f"min = {np.max(data):.2e} {bunit}"

    fig, ax = plt.subplots(figsize=(4, 4))
    ax.imshow(
        data, origin="lower", cmap="viridis",
        vmin=-np.nanpercentile(np.where(data < 0.0, -data, np.nan), 10.0),
        vmax=np.percentile(data, 99.)
    )
    ax.set_axis_off()
    ax.annotate(
        f"{max_str}\n{min_str}\n{std_str}", 
        xy=(0.95, 0.95), xycoords='axes fraction',
        ha='right', va='top', color='white'
    )
    buf = io.BytesIO()

    fig.savefig(buf, format="png", bbox_inches="tight", pad_inches=0)
    plt.close(fig)
    return buf.getvalue()


def _placeholder_png(message: str) -> bytes:
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.text(0.5, 0.5, message, ha="center", va="center", wrap=True)
    ax.set_axis_off()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


def create_app(store: StateStore) -> FastAPI:
    app = FastAPI(title="Mini-SDP Quality Assessment")

    @app.get("/api/datasets")
    def list_pending():
        out = []
        for qa in store.list_pending_qa():
            pd = store.get_processed_dataset(qa.processed_dataset_id)
            if pd is None:
                continue
            pb = store.get_processing_block_by_processed_dataset(pd.id)
            out.append({
                "qa_id": qa.id,
                "dataset_id": pd.id,
                "observation_id": pb.observing_block_id if pb else None,
                "path": str(pd.path),
                "size": pd.size,
            })
        return out

    @app.post("/api/qa/{qa_id}/approve")
    def approve(qa_id: int):
        if store.get_qa_assessment(qa_id) is None:
            raise HTTPException(404, "assessment not found")
        store.set_qa_status(qa_id, QAStatus.APPROVED)
        return {"qa_id": qa_id, "status": QAStatus.APPROVED}

    @app.post("/api/qa/{qa_id}/reprocess")
    def reprocess(qa_id: int):
        if store.get_qa_assessment(qa_id) is None:
            raise HTTPException(404, "assessment not found")
        store.set_qa_status(qa_id, QAStatus.REJECTED)
        return {"qa_id": qa_id, "status": QAStatus.REJECTED}

    @app.get("/api/datasets/{dataset_id}/preview.png")
    def preview(dataset_id: int):
        pd = store.get_processed_dataset(dataset_id)
        if pd is None:
            raise HTTPException(404, "dataset not found")
        image = _find_image_fits(Path(pd.path))
        png = _render_fits_png(image) if image else _placeholder_png("No image available")
        return Response(content=png, media_type="image/png")

    @app.get("/", response_class=HTMLResponse)
    def index():
        return _INDEX_HTML

    return app


_INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mini-SDP Quality Assessment</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #0f1116; color: #e6e6e6; }
  h1 { font-weight: 600; }
  #empty { color: #8a8f98; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 1rem; }
  .card { background: #1a1d26; border: 1px solid #2a2e3a; border-radius: 8px; padding: 1rem; }
  /* Reserve a square box before the PNG loads so the card never resizes (no layout shift). */
  .card img { display: block; width: 100%; aspect-ratio: 1 / 1; object-fit: contain;
              border-radius: 4px; background: #000; }
  .meta { font-size: 0.85rem; color: #b6bcc8; margin: 0.5rem 0; }
  .actions { display: flex; gap: 0.5rem; }
  button {
    flex: 1;
    padding: 0.5rem;
    border: 0;
    border-radius: 4px; 
    cursor: pointer;
    font-weight: 600;
    transition: background 0.15s ease, color 0.15s ease;
  }
  .approve { background: #2f9e44; color: #fff; }
  .approve:hover { background: #2b8a3e; }
  .approve:active { background: #111; }
  .reprocess { background: #e8590c; color: #fff; }
  .reprocess:hover { background: #ca4d0a; }
  .refresh {
    flex: 0 0 auto;
    width: 2rem; height: 2rem;
    padding: 0;
    margin-left: 0.6rem;
    border-radius: 50%;
    border: 1px solid #2a2e3a;
    background: #1a1d26;
    color: #b6bcc8;
    font-size: 1.15rem;
    line-height: 1;
    vertical-align: middle;
    transition: background 0.15s ease, color 0.15s ease, transform 0.5s ease;
  }
  .refresh:hover { background: #2a2e3a; color: #fff; }
  .refresh:active { transform: rotate(360deg); }  /* spin once on click */
</style>
</head>
<body>
<h1>Quality Assessment <button class="refresh" onclick="refresh()" title="Refresh" aria-label="Refresh">↻</button></h1>
<p id="empty">Loading…</p>
<div class="grid" id="grid"></div>
<script>
async function refresh() {
  const data = await (await fetch('/api/datasets')).json();
  const grid = document.getElementById('grid');
  const empty = document.getElementById('empty');
  empty.textContent = data.length ? '' : 'No products awaiting assessment.';
  grid.innerHTML = '';
  for (const d of data) {
    const card = document.createElement('div');
    card.className = 'card';
    card.innerHTML = `
      <img src="/api/datasets/${d.dataset_id}/preview.png?t=${Date.now()}" alt="preview">
      <div class="meta">Observation ${d.observation_id} · dataset ${d.dataset_id}<br>${d.path}</div>
      <div class="actions">
        <button class="approve" onclick="act(${d.qa_id}, 'approve')">Approve</button>
        <button class="reprocess" onclick="act(${d.qa_id}, 'reprocess')">Re-process</button>
      </div>`;
    grid.appendChild(card);
  }
}
async function act(id, what) {
  await fetch(`/api/qa/${id}/${what}`, { method: 'POST' });
  refresh();
}
refresh();
//setInterval(refresh, 5000);
</script>
</body>
</html>"""
