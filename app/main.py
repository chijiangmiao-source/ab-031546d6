"""Entry point: initialise durable state, resume any interrupted migrations,
serve the API. Everything after a crash is reconstructed from SQLite."""
from __future__ import annotations

import os
from wsgiref.simple_server import make_server

from . import db, engine
from .api import application


def main() -> None:
    db.init_db()
    # Crash recovery: converge purely from persisted state before accepting
    # traffic. Any migration left mid-flight by a previous (killed) process
    # is resumed here and by the background worker.
    engine.advance_all()
    engine.start_worker(interval=float(os.environ.get("WORKER_INTERVAL", "0.2")))
    port = int(os.environ.get("PORT", "8080"))
    httpd = make_server("0.0.0.0", port, application)
    print(f"Serving on 0.0.0.0:{port}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
