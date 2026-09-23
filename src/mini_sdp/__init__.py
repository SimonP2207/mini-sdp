"""Entry point for the Data Orchestrator process.

Wires the State Store, Storage Manager, Observer, Processor, and Archiver together and
runs the observing + QA-reconcile loops. The QA web app (``mini-sdp-qa``) and the
observation generator (``mini-sdp-generate``) run as separate processes sharing ``--db``.
"""
import argparse
import asyncio
import logging
import signal
from pathlib import Path

from .archiver import DataArchiver, LocalArchive
from .data.store import StateStore
from .observer import DataObserver
from .orchestrator import DataOrchestrator
from .processor import DataProcessor
from .storage import LocalStorage, StorageManager

GIB = 1024 ** 3


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini-SDP Data Orchestrator")
    parser.add_argument("--data-dir", type=Path, default=Path("data"),
                        help="Root directory for raw and processed data storage")
    parser.add_argument("--db", type=Path, default=Path("sdp.db"),
                        help="Path to the shared State Store SQLite database")
    parser.add_argument("--threshold", type=int, default=3 * GIB,
                        help="Storage threshold in bytes; observing stops once reached")
    parser.add_argument("--disk-margin", type=int, default=1 * GIB,
                        help="Bytes of real free disk to keep in reserve")
    parser.add_argument("--max-parallel", type=int, default=2,
                        help="Maximum concurrent processing runs")
    parser.add_argument("--archive-dir", type=Path, default=None,
                        help="Product Archive directory (default: <data-dir>/archive)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    store = StateStore(args.db)
    store.init_schema()

    storage = LocalStorage(args.data_dir, capacity_bytes=args.threshold)
    manager = StorageManager(storage, store, disk_margin_bytes=args.disk_margin)
    archive_dir = args.archive_dir or (args.data_dir / "archive")
    archiver = DataArchiver(LocalArchive(archive_dir))

    orchestrator = DataOrchestrator(
        store, manager, DataObserver(), DataProcessor(), archiver,
        max_parallel=args.max_parallel,
    )

    logging.getLogger(__name__).info(
        "Orchestrator starting: db=%s data=%s threshold=%d bytes max_parallel=%d",
        args.db, args.data_dir, args.threshold, args.max_parallel,
    )

    async def _serve() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, orchestrator.stop)
            except NotImplementedError:
                pass  # Windows event loop doesn't support signal handlers
        await orchestrator.run()

    try:
        asyncio.run(_serve())
    except KeyboardInterrupt:
        pass
    finally:
        store.close()
