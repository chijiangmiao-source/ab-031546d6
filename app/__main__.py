from __future__ import annotations

import argparse
import os

from .server import serve


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    db_path = os.environ.get("DB_PATH", "/data/archive.db")
    ap = argparse.ArgumentParser(description="Archive migration station")
    ap.add_argument("--host", default=host)
    ap.add_argument("--port", type=int, default=port)
    ap.add_argument("--db", default=db_path)
    args = ap.parse_args()
    serve(args.host, args.port, args.db)


if __name__ == "__main__":
    main()
