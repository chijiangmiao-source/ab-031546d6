#!/bin/sh
# Compose verify entrypoint: code tests + image sanity + HTTP smoke.
# The image itself is built by `docker compose build` / `up --build`
# before this one-shot service starts.
set -e

echo "=== 1/3: code tests (migration consistency incl. hard crash/reboot) ==="
python -m unittest discover -s tests -p 'test_*.py' -v

echo "=== 2/3: image sanity (modules importable in the built image) ==="
python -c "import app.server, app.migration, app.storage; print('image OK: app modules import')"

echo "=== 3/3: HTTP smoke against ${BASE_URL:-http://web:8080} ==="
exec python3 tests/smoke_http.py
