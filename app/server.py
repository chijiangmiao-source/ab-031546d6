"""Remote sensing archive station – migration rehearsal HTTP API.

Pure standard library so the container image builds with no network access.
"""

from __future__ import annotations

import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .migration import MigrationError, MigrationService
from .storage import CrashNow, open_store

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
START_TIME = time.time()


def make_handler(service: MigrationService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ArchiveStation/1.0"

        def log_message(self, fmt: str, *args) -> None:  # noqa: A003
            sys.stderr.write(
                "%s - %s\n" % (self.address_string(), fmt % args)
            )

        # ------------------------------------------------------------- helpers
        def _json(self, obj, status: int = 200) -> None:
            body = json.dumps(obj, ensure_ascii=False, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            try:
                n = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                n = 0
            raw = self.rfile.read(n) if n else b"{}"
            try:
                data = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                raise MigrationError(400, "bad_json", "请求体不是合法 JSON")
            if not isinstance(data, dict):
                raise MigrationError(400, "bad_json", "请求体必须是 JSON 对象")
            return data

        def _static(self, rel: str, ctype: str) -> None:
            path = os.path.normpath(os.path.join(STATIC_DIR, rel))
            if not path.startswith(STATIC_DIR) or not os.path.isfile(path):
                self._json({"error": "not_found"}, 404)
                return
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _error(self, e: MigrationError) -> None:
            self._json({"error": e.code, "message": e.message}, e.status)

        def _hard_exit(self) -> None:
            # The crash point fires after fsync; emulate sudden power loss.
            sys.stderr.write("crash point reached – hard exit\n")
            sys.stderr.flush()
            os._exit(7)

        # --------------------------------------------------------------- GET
        def do_GET(self) -> None:  # noqa: N802
            p = urlparse(self.path).path
            try:
                if p == "/healthz" or p == "/api/health":
                    self._json(
                        {
                            "status": "ok",
                            "uptime_sec": round(time.time() - START_TIME, 2),
                            "db": service.store.db_path,
                            "drills": len(service.store.list_drills()),
                            "time": time.time(),
                        }
                    )
                    return
                if p == "/" or p == "/index.html":
                    self._static("index.html", "text/html; charset=utf-8")
                    return
                if p == "/static/app.js":
                    self._static("app.js", "application/javascript; charset=utf-8")
                    return
                if p == "/static/styles.css":
                    self._static("styles.css", "text/css; charset=utf-8")
                    return
                if p == "/api/drills":
                    self._json({"drills": service.store.list_drills()})
                    return
                if p.startswith("/api/drills/"):
                    parts = p.strip("/").split("/")
                    if len(parts) == 3:
                        self._json(self._status(parts[2]))
                        return
                self._json({"error": "not_found", "path": p}, 404)
            except MigrationError as e:
                self._error(e)
            except CrashNow:
                self._hard_exit()
            except Exception as e:  # pragma: no cover
                self._json({"error": "internal", "message": str(e)}, 500)

        # -------------------------------------------------------------- POST
        def do_POST(self) -> None:  # noqa: N802
            p = urlparse(self.path).path
            parts = p.strip("/").split("/")
            try:
                if p == "/api/drills":
                    data = self._read_json()
                    did = service.create_drill(
                        data.get("spec", data), data.get("drill_id")
                    )
                    self._json({"drill_id": did}, 201)
                    return
                if len(parts) == 4 and parts[0] == "api" and parts[1] == "drills":
                    did, action = parts[2], parts[3]
                    if service.store.get_drill(did) is None:
                        raise MigrationError(404, "not_found", "演练不存在")
                    data = self._read_json()
                    if action == "migration":
                        self._json(service.start(did, str(data.get("migration_id", ""))))
                        return
                    if action == "writes":
                        op = data.get("op", "upsert")
                        token = str(data.get("token") or f"manual:{time.time()}")
                        upd = {
                            "op": op,
                            "rid": str(data.get("rid", "")),
                            "batch": int(data.get("batch", 0) or 0),
                            "payload": None if op == "delete"
                            else data.get("payload", ""),
                        }
                        if upd["op"] not in ("upsert", "update", "delete"):
                            raise MigrationError(400, "bad_op", "未知操作")
                        if not upd["rid"]:
                            raise MigrationError(400, "bad_rid", "缺少 rid")
                        res = service.ingest_write(did, token, upd)
                        service.spawn_worker(did)
                        self._json(res, 202)
                        return
                    if action == "resume":
                        service.spawn_worker(did)
                        self._json({"resumed": True}, 202)
                        return
                self._json({"error": "not_found", "path": p}, 404)
            except MigrationError as e:
                self._error(e)
            except CrashNow:
                self._hard_exit()
            except Exception as e:  # pragma: no cover
                self._json({"error": "internal", "message": str(e)}, 500)

        def _status(self, drill_id: str) -> dict:
            spec = service.store.get_drill(drill_id)
            if spec is None:
                raise MigrationError(404, "not_found", "演练不存在")
            row = service.store.get_migration_row(drill_id)
            return {
                "spec": spec,
                "source": service.store.source_rows(drill_id),
                "snapshot": service.store.snapshot_rows(drill_id),
                "target": service.store.target_rows(drill_id),
                "increments": service.store.increments(drill_id),
                "migration": dict(row) if row else None,
                "coverage": service.store.coverage(drill_id),
                "events": service.events(drill_id, 200),
            }

    return Handler


def serve(host: str, port: int, db_path: str) -> None:
    store = open_store(db_path)
    service = MigrationService(store)
    service.recover_on_boot()
    httpd = ThreadingHTTPServer((host, port), make_handler(service))
    actual_port = httpd.server_address[1]
    print(f"archive station listening on http://{host}:{actual_port} "
          f"(db={db_path})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()
