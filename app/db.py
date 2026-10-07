"""Persistence layer for the remote-sensing archive migration drill.

Every piece of progress that must survive a crash lives in SQLite (WAL mode):
the migration intent keyed by an idempotency id, the frozen snapshot watermark,
completed snapshot pages, fired source-update batches, the ordered increment
journal and the read pointer.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = os.environ.get("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data"))
DB_PATH = str(Path(DATA_DIR) / "migration.db")

# Migration states (the state machine).
INTENT = "INTENT"                    # submit durable, boundary not frozen yet
FROZEN = "FROZEN"                    # snapshot watermark frozen
COPYING = "COPYING"                  # snapshot pages being copied
CATCHING_UP = "CATCHING_UP"          # snapshot done, increments projected
SWITCH_PENDING = "SWITCH_PENDING"    # switch intent durable, pointer not moved
SWITCHED = "SWITCHED"                # terminal: read pointer atomically moved

TERMINAL = {SWITCHED}

SCHEMA = """
CREATE TABLE IF NOT EXISTS drill (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    config      TEXT NOT NULL,           -- immutable drill definition (JSON)
    page_size   INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_record (
    drill_id   TEXT NOT NULL,
    rid        TEXT NOT NULL,
    payload    TEXT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,         -- source write sequence number
    PRIMARY KEY (drill_id, rid)
);

CREATE TABLE IF NOT EXISTS source_counter (
    drill_id TEXT PRIMARY KEY,
    counter  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS migration (
    drill_id     TEXT PRIMARY KEY REFERENCES drill(id),
    migration_id TEXT NOT NULL UNIQUE,    -- idempotency key
    state        TEXT NOT NULL,
    watermark    INTEGER,                 -- frozen snapshot boundary
    pages_total  INTEGER,
    pointer      TEXT NOT NULL DEFAULT 'source',
    started_at   TEXT,
    switched_at  TEXT
);

CREATE TABLE IF NOT EXISTS increment (
    drill_id TEXT NOT NULL,
    seq      INTEGER NOT NULL,            -- ordered post-boundary write
    rid      TEXT NOT NULL,
    payload  TEXT NOT NULL,
    version  INTEGER NOT NULL,
    applied  INTEGER NOT NULL DEFAULT 0,  -- projected exactly once
    PRIMARY KEY (drill_id, seq)
);

CREATE TABLE IF NOT EXISTS target_record (
    drill_id   TEXT NOT NULL,
    rid        TEXT NOT NULL,
    payload    TEXT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    seq        INTEGER NOT NULL,          -- source write sequence represented here
    PRIMARY KEY (drill_id, rid)
);

CREATE TABLE IF NOT EXISTS snap_row (
    -- immutable frozen image of the source at the boundary watermark
    drill_id   TEXT NOT NULL,
    rid        TEXT NOT NULL,
    payload    TEXT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (drill_id, rid)
);

CREATE TABLE IF NOT EXISTS snap_page (
    drill_id TEXT NOT NULL,
    page_no  INTEGER NOT NULL,
    last_rid TEXT NOT NULL,
    PRIMARY KEY (drill_id, page_no)
);

CREATE TABLE IF NOT EXISTS fired_batch (
    drill_id TEXT NOT NULL,
    batch_no INTEGER NOT NULL,
    PRIMARY KEY (drill_id, batch_no)
);

CREATE TABLE IF NOT EXISTS crash_mark (
    drill_id TEXT NOT NULL,
    point    TEXT NOT NULL,
    PRIMARY KEY (drill_id, point)
);

CREATE TABLE IF NOT EXISTS event (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    drill_id TEXT NOT NULL,
    at       TEXT NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT NOT NULL DEFAULT ''
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str | None = None) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)


def log_event(conn: sqlite3.Connection, drill_id: str, kind: str, detail: str = "") -> None:
    conn.execute(
        "INSERT INTO event(drill_id, at, kind, detail) VALUES (?,?,?,?)",
        (drill_id, utcnow(), kind, detail),
    )


def get_drill(conn: sqlite3.Connection, drill_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM drill WHERE id=?", (drill_id,)).fetchone()


def get_config(conn: sqlite3.Connection, drill_id: str) -> dict:
    row = get_drill(conn, drill_id)
    if row is None:
        raise KeyError(drill_id)
    return json.loads(row["config"])


def get_migration(conn: sqlite3.Connection, drill_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM migration WHERE drill_id=?", (drill_id,)
    ).fetchone()
