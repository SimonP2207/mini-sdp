"""Entry point for the standalone QA web app process."""
import argparse
from pathlib import Path

import uvicorn

from ..data.store import StateStore
from .app import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini-SDP Quality Assessment web app")
    parser.add_argument("--db", type=Path, default=Path("sdp.db"),
                        help="Path to the shared State Store SQLite database")
    parser.add_argument("--data-dir", type=Path, default=Path("data"),
                        help="Root of the data storage (for reference)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    store = StateStore(args.db)
    store.init_schema()
    app = create_app(store)
    uvicorn.run(app, host=args.host, port=args.port)
