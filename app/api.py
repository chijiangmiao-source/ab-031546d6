"""HTTP API for the migration drill — real endpoints, no mocks."""
from __future__ import annotations

import json
import os
import sqlite3
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlparse

from . import db, engine

STATIC_DIR = Path(__file__).resolve().parent / "static"

STARTED_AT = db.utcnow()


def _json(start_response, status: int, payload) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    start_response(
        f"{status} {HTTPStatus(status).phrase}",
        [("Content-Type", "application/json; charset=utf-8"),
         ("Content-Length", str(len(body)))],
    )(body)


def _read_json(environ) -> dict:
    length = int(environ.get("CONTENT_LENGTH") or 0)
    if length <= 0:
        return {}
    raw = environ["wsgi.input"].read(length)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise engine.DrillError(f"invalid JSON body: {e}")
    if not isinstance(data, dict):
        raise engine.DrillError("JSON body must be an object")
    return data


def _projection(conn, drill_id: str, table: str, limit: int = 8) -> dict:
    total = conn.execute(
        f"SELECT COUNT(*) c FROM {table} WHERE drill_id=?", (drill_id,)
    ).fetchone()["c"]
    if table == "source_record":
        rows = conn.execute(
            "SELECT rid, payload, version, updated_at, updated_at AS seq "
            "FROM source_record WHERE drill_id=? ORDER BY rid LIMIT ?",
            (drill_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT rid, payload, version, updated_at, seq FROM target_record "
            "WHERE drill_id=? ORDER BY rid LIMIT ?",
            (drill_id, limit),
        ).fetchall()
    return {"count": total, "rows": [dict(r) for r in rows]}


def _status(conn, drill_id: str) -> dict:
    drill = db.get_drill(conn, drill_id)
    if drill is None:
        raise engine.DrillError("drill not found")
    mig = db.get_migration(conn, drill_id)
    config = json.loads(drill["config"])
    inc_total = conn.execute(
        "SELECT COUNT(*) c FROM increment WHERE drill_id=?", (drill_id,)
    ).fetchone()["c"]
    inc_applied = conn.execute(
        "SELECT COUNT(*) c FROM increment WHERE drill_id=? AND applied=1", (drill_id,)
    ).fetchone()["c"]
    max_seq = conn.execute(
        "SELECT MAX(seq) m FROM increment WHERE drill_id=?", (drill_id,)
    ).fetchone()["m"]
    events = conn.execute(
        "SELECT at, kind, detail FROM event WHERE drill_id=? ORDER BY id DESC LIMIT 14",
        (drill_id,),
    ).fetchall()
    fired = [
        r["batch_no"] for r in conn.execute(
            "SELECT batch_no FROM fired_batch WHERE drill_id=? ORDER BY batch_no",
            (drill_id,),
        ).fetchall()
    ]
    pages_done = conn.execute(
        "SELECT COUNT(*) c FROM snap_page WHERE drill_id=?", (drill_id,)
    ).fetchone()["c"]
    counter = conn.execute(
        "SELECT counter FROM source_counter WHERE drill_id=?", (drill_id,)
    ).fetchone()["counter"]
    conclusion = None
    if mig is not None and mig["state"] == db.SWITCHED:
        gap = conn.execute(
            "SELECT COUNT(*) c FROM source_record s LEFT JOIN target_record t "
            "ON t.drill_id=s.drill_id AND t.rid=s.rid "
            "WHERE s.drill_id=? AND (t.rid IS NULL OR t.seq<>s.updated_at "
            "OR t.version<>s.version OR t.payload<>s.payload)",
            (drill_id,),
        ).fetchone()["c"]
        dup_applied = conn.execute(
            "SELECT COUNT(*) c FROM (SELECT rid FROM target_record WHERE drill_id=? GROUP BY rid HAVING COUNT(*)>1)",
            (drill_id,),
        ).fetchone()["c"]
        conclusion = {
            "switched": gap == 0,
            "divergent_rows": gap,
            "duplicate_target_rows": dup_applied,
            "verdict": "CONSISTENT: target covers snapshot and all increments; pointer atomically switched"
            if gap == 0 else "INCONSISTENT",
        }
    return {
        "drill_id": drill_id,
        "name": drill["name"],
        "config": config,
        "created_at": drill["created_at"],
        "migration": None if mig is None else {
            "migration_id": mig["migration_id"],
            "state": mig["state"],
            "watermark": mig["watermark"],
            "pages_done": pages_done,
            "pages_total": mig["pages_total"],
            "pointer": mig["pointer"],
            "started_at": mig["started_at"],
            "switched_at": mig["switched_at"],
        },
        "source_write_counter": counter,
        "increments": {"total": inc_total, "applied": inc_applied, "max_seq": max_seq},
        "fired_batches": fired,
        "source_projection": _projection(conn, drill_id, "source_record"),
        "target_projection": _projection(conn, drill_id, "target_record"),
        "events": [dict(r) for r in events],
        "conclusion": conclusion,
    }


# --------------------------------------------------------------------------- #
# Routing
# --------------------------------------------------------------------------- #

def application(environ, start_response):
    method = environ["REQUEST_METHOD"]
    path = urlparse(environ["PATH_INFO"]).path
    try:
        if method == "GET" and path == "/health":
            with db.connect() as conn:
                conn.execute("SELECT 1").fetchone()
            return _json(start_response, 200, {
                "status": "ok",
                "service": "remote-sensing-migration-drill",
                "started_at": STARTED_AT,
                "db": "wal",
            })

        if method == "GET" and path == "/":
            return _serve_static(start_response, "index.html", "text/html; charset=utf-8")

        if method == "GET" and path.startswith("/static/"):
            name = path.split("/static/", 1)[1]
            ctype = "application/javascript" if name.endswith(".js") else "text/plain"
            return _serve_static(start_response, name, ctype)

        if method == "GET" and path == "/api/drills":
            with db.connect() as conn:
                rows = conn.execute(
                    "SELECT d.id, d.name, m.state, m.pointer, m.watermark "
                    "FROM drill d LEFT JOIN migration m ON m.drill_id=d.id ORDER BY d.created_at"
                ).fetchall()
            return _json(start_response, 200, {"drills": [dict(r) for r in rows]})

        if method == "POST" and path == "/api/drills":
            data = _read_json(environ)
            drill_id = engine.create_drill(
                name=data.get("name", ""),
                initial_records=data.get("initial_records", 10),
                page_size=data.get("page_size", 4),
                batches=data.get("batches", []),
                crash_point=data.get("crash_point"),
            )
            with db.connect() as conn:
                payload = _status(conn, drill_id)
            return _json(start_response, 201, payload)

        if method == "GET" and path.startswith("/api/drills/"):
            drill_id = path.rsplit("/", 1)[-1]
            with db.connect() as conn:
                return _json(start_response, 200, _status(conn, drill_id))

        if method == "POST" and path.endswith("/migrate"):
            drill_id = path.split("/")[3]
            data = _read_json(environ)
            result = engine.submit_migration(drill_id, data.get("migration_id", ""))
            # nudge immediately; worker keeps converging afterwards
            engine.advance(drill_id)
            with db.connect() as conn:
                payload = _status(conn, drill_id)
            payload["submit"] = result
            return _json(start_response, 200, payload)

        if method == "POST" and path.endswith("/batches/fire"):
            drill_id = path.split("/")[3]
            data = _read_json(environ)
            result = engine.fire_batch(drill_id, int(data.get("batch_no", 0)))
            engine.advance(drill_id)
            with db.connect() as conn:
                payload = _status(conn, drill_id)
            payload["fire"] = result
            return _json(start_response, 200, payload)

        return _json(start_response, 404, {"error": "not found", "path": path})

    except engine.DrillError as e:
        return _json(start_response, 409 if isinstance(e, engine.MigrationConflict) else 400,
                     {"error": str(e)})
    except KeyError:
        return _json(start_response, 404, {"error": "drill not found"})
    except sqlite3.Error as e:
        return _json(start_response, 500, {"error": f"database error: {e}"})


def _serve_static(start_response, name: str, ctype: str):
    # prevent path traversal
    safe = (STATIC_DIR / name).resolve()
    if not str(safe).startswith(str(STATIC_DIR.resolve())) or not safe.is_file():
        body = b"not found"
        start_response("404 Not Found", [("Content-Length", str(len(body)))])
        return [body]
    body = safe.read_bytes()
    start_response("200 OK", [("Content-Type", ctype), ("Content-Length", str(len(body)))])
    return [body]
