"""Persistence layer for the archive index migration rehearsal.

Everything that must survive a process crash lives in SQLite:

* the immutable drill specification (submission time = snapshot boundary),
* source rows written by the *old* collector (before and after the boundary),
* the ordered changelog of post-boundary writes,
* migration progress: snapshot copied flag, last applied increment seq,
  switch intent, atomic read pointer.

Incremental writes are persisted *before* they can be projected, and the
read pointer flips in the same transaction that confirms the target covers
the snapshot plus every recorded increment ("exactly once", ordered).
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS drills (
    id TEXT PRIMARY KEY,
    spec_json TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS source_rows (
    drill_id TEXT NOT NULL,
    rid TEXT NOT NULL,
    batch INTEGER,
    payload TEXT NOT NULL,
    written_at REAL NOT NULL,
    PRIMARY KEY (drill_id, rid)
);
CREATE TABLE IF NOT EXISTS increments (
    drill_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    op TEXT NOT NULL,
    rid TEXT NOT NULL,
    batch INTEGER,
    payload TEXT,
    recorded_at REAL NOT NULL,
    PRIMARY KEY (drill_id, seq)
);
CREATE TABLE IF NOT EXISTS target_rows (
    drill_id TEXT NOT NULL,
    rid TEXT NOT NULL,
    batch INTEGER,
    payload TEXT NOT NULL,
    updated_seq INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (drill_id, rid)
);
CREATE TABLE IF NOT EXISTS snapshot_rows (
    drill_id TEXT NOT NULL,
    rid TEXT NOT NULL,
    batch INTEGER,
    payload TEXT NOT NULL,
    written_at REAL NOT NULL,
    PRIMARY KEY (drill_id, rid)
);
CREATE TABLE IF NOT EXISTS migrations (
    drill_id TEXT PRIMARY KEY,
    migration_id TEXT UNIQUE,
    snapshot_done INTEGER NOT NULL DEFAULT 0,
    last_applied_seq INTEGER NOT NULL DEFAULT 0,
    read_pointer TEXT NOT NULL DEFAULT 'source',
    switch_intended INTEGER NOT NULL DEFAULT 0,
    switched_at REAL
);
"""

# Named crash points that a drill may arm. Each fires at most once, *after*
# the relevant commit has been fsynced, so it emulates a real power loss.
CRASH_AFTER_DRILL_SUBMIT = "after_drill_submit"
CRASH_AFTER_FREEZE = "after_freeze"
CRASH_AFTER_INCREMENT = "after_increment"
CRASH_AFTER_SNAPSHOT = "after_snapshot"
CRASH_AFTER_PROJECT = "after_project"
CRASH_AFTER_SWITCH_INTENT = "after_switch_intent"
CRASH_AFTER_SWITCH = "after_switch"
ALL_CRASH_POINTS = [
    CRASH_AFTER_DRILL_SUBMIT,
    CRASH_AFTER_FREEZE,
    CRASH_AFTER_INCREMENT,
    CRASH_AFTER_SNAPSHOT,
    CRASH_AFTER_PROJECT,
    CRASH_AFTER_SWITCH_INTENT,
    CRASH_AFTER_SWITCH,
]


class CrashNow(Exception):
    """Raised at an armed crash point to simulate a hard process exit."""


class CutoverComplete(Exception):
    """The read pointer is already on target; the old collector is retired."""


class Store:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # ------------------------------------------------------------------ drills
    def create_drill(self, drill_id: str, spec: dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO drills (id, spec_json, created_at) "
                "VALUES (?,?,?)",
                (drill_id, json.dumps(spec, sort_keys=True), time.time()),
            )
            # Initial records live in the source table *before* the boundary.
            for r in spec["records"]:
                self.conn.execute(
                    "INSERT OR IGNORE INTO source_rows "
                    "(drill_id, rid, batch, payload, written_at) "
                    "VALUES (?,?,?,?,?)",
                    (drill_id, r["rid"], None, r["payload"], time.time()),
                )
            self.conn.commit()

    def list_drills(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT id, spec_json, created_at FROM drills ORDER BY created_at"
            ).fetchall()
        out = []
        for r in rows:
            spec = json.loads(r["spec_json"])
            spec["drill_id"] = r["id"]
            spec["created_at"] = r["created_at"]
            out.append(spec)
        return out

    def get_drill(self, drill_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT spec_json FROM drills WHERE id=?", (drill_id,)
            ).fetchone()
        if not row:
            return None
        spec = json.loads(row["spec_json"])
        spec["drill_id"] = drill_id
        return spec

    def get_spec_raw(self, drill_id: str) -> Optional[str]:
        with self._lock:
            row = self.conn.execute(
                "SELECT spec_json FROM drills WHERE id=?", (drill_id,)
            ).fetchone()
        return row["spec_json"] if row else None

    # ------------------------------------------------------------- old writer
    def apply_old_write(self, drill_id: str, upd: dict[str, Any]) -> dict[str, Any]:
        """Persist one old-collector write.

        Before the boundary freezes, the write lands in the source table
        only. After freeze it is captured in the durable ordered changelog
        *and* applied to the live source table (the old collector never
        stops writing).
        """
        with self._lock:
            mig = self.get_migration_row(drill_id)
            if mig is not None and mig["read_pointer"] == "target":
                # Cutover is atomic and final: after the pointer flips the old
                # collector is retired, so its late writes cannot land.
                raise CutoverComplete(
                    "read pointer already on target; old collector retired")
            frozen = mig is not None
            seq = None
            if frozen:
                seq = self.conn.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS n FROM increments "
                    "WHERE drill_id=?",
                    (drill_id,),
                ).fetchone()["n"]
                self.conn.execute(
                    "INSERT INTO increments "
                    "(drill_id, seq, op, rid, batch, payload, recorded_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        drill_id,
                        seq,
                        upd["op"],
                        upd["rid"],
                        upd.get("batch"),
                        upd.get("payload"),
                        time.time(),
                    ),
                )
            if upd["op"] in ("upsert", "update"):
                self.conn.execute(
                    "INSERT INTO source_rows "
                    "(drill_id, rid, batch, payload, written_at) "
                    "VALUES (?,?,?,?,?) "
                    "ON CONFLICT(drill_id, rid) DO UPDATE SET "
                    "batch=excluded.batch, payload=excluded.payload, "
                    "written_at=excluded.written_at",
                    (drill_id, upd["rid"], upd.get("batch"),
                     upd.get("payload", ""), time.time()),
                )
            elif upd["op"] == "delete":
                self.conn.execute(
                    "DELETE FROM source_rows WHERE drill_id=? AND rid=?",
                    (drill_id, upd["rid"]),
                )
            else:
                raise ValueError(f"unknown op {upd['op']!r}")
            self.conn.commit()
            return {"frozen": frozen, "seq": seq}

    def increments(self, drill_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT seq, op, rid, batch, payload, recorded_at "
                "FROM increments WHERE drill_id=? ORDER BY seq",
                (drill_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def source_rows(self, drill_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT rid, batch, payload, written_at "
                "FROM source_rows WHERE drill_id=? ORDER BY rid",
                (drill_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # -------------------------------------------------------------- migration
    def get_migration_row(self, drill_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM migrations WHERE drill_id=?", (drill_id,)
            ).fetchone()

    def freeze_boundary(self, drill_id: str, migration_id: str) -> bool:
        """Freeze the replication boundary.

        Returns True if a brand-new migration was created, False if this
        exact migration id was already registered for this drill (idempotent
        retransmit). Re-using an id for a different drill is rejected.
        """
        with self._lock:
            row = self.conn.execute(
                "SELECT drill_id FROM migrations WHERE migration_id=?",
                (migration_id,),
            ).fetchone()
            if row is not None:
                if row["drill_id"] != drill_id:
                    raise ValueError("migration_id already used by another drill")
                return False
            self.conn.execute(
                "INSERT INTO migrations (drill_id, migration_id) VALUES (?,?)",
                (drill_id, migration_id),
            )
            # Freeze a point-in-time copy of the source in the SAME
            # transaction: the snapshot and the boundary are atomic, so no
            # old-collector write can straddle them.
            self.conn.execute(
                "INSERT INTO snapshot_rows "
                "(drill_id, rid, batch, payload, written_at) "
                "SELECT drill_id, rid, batch, payload, written_at "
                "FROM source_rows WHERE drill_id=?",
                (drill_id,),
            )
            self.conn.commit()
            return True

    def snapshot_state(self, drill_id: str) -> bool:
        with self._lock:
            row = self.get_migration_row(drill_id)
            return bool(row and row["snapshot_done"])

    def mark_snapshot_done(self, drill_id: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE migrations SET snapshot_done=1 WHERE drill_id=?",
                (drill_id,),
            )
            self.conn.commit()

    def pending_increments(self, drill_id: str) -> list[dict[str, Any]]:
        with self._lock:
            last = self.get_migration_row(drill_id)["last_applied_seq"]
            rows = self.conn.execute(
                "SELECT seq, op, rid, batch, payload FROM increments "
                "WHERE drill_id=? AND seq>? ORDER BY seq",
                (drill_id, last),
            ).fetchall()
        return [dict(r) for r in rows]

    def snapshot_rows(self, drill_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT rid, batch, payload, written_at "
                "FROM snapshot_rows WHERE drill_id=? ORDER BY rid",
                (drill_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def copy_snapshot_into_target(self, drill_id: str) -> int:
        """Copy the frozen snapshot table into the target table."""
        with self._lock:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO target_rows "
                "(drill_id, rid, batch, payload, updated_seq) "
                "SELECT drill_id, rid, batch, payload, 0 "
                "FROM snapshot_rows WHERE drill_id=?",
                (drill_id,),
            )
            n = cur.rowcount
            self.conn.commit()
        return n

    def project_increment(self, drill_id: str, inc: dict[str, Any]) -> None:
        """Project one recorded increment into the target, exactly once.

        The row change and last_applied_seq commit together, so a crash can
        neither project an increment twice nor skip one. Target state is
        derived from the immutable changelog, never re-read from source.
        """
        with self._lock:
            if inc["op"] in ("upsert", "update"):
                done = self.conn.execute(
                    "SELECT 1 FROM target_rows WHERE drill_id=? AND rid=?",
                    (drill_id, inc["rid"]),
                ).fetchone()
                if done:
                    self.conn.execute(
                        "UPDATE target_rows SET batch=?, payload=?, updated_seq=? "
                        "WHERE drill_id=? AND rid=?",
                        (inc.get("batch"), inc.get("payload", ""), inc["seq"],
                         drill_id, inc["rid"]),
                    )
                else:
                    self.conn.execute(
                        "INSERT INTO target_rows "
                        "(drill_id, rid, batch, payload, updated_seq) "
                        "VALUES (?,?,?,?,?)",
                        (drill_id, inc["rid"], inc.get("batch"),
                         inc.get("payload", ""), inc["seq"]),
                    )
            else:  # delete
                self.conn.execute(
                    "DELETE FROM target_rows WHERE drill_id=? AND rid=?",
                    (drill_id, inc["rid"]),
                )
            cur = self.conn.execute(
                "UPDATE migrations SET last_applied_seq=? WHERE drill_id=? "
                "AND last_applied_seq=?",
                (inc["seq"], drill_id, inc["seq"] - 1),
            )
            if cur.rowcount != 1:
                # Out-of-order or duplicate projection: abort the txn rather
                # than risk double application or a gap in the sequence.
                self.conn.rollback()
                raise RuntimeError(
                    f"increment seq gap for drill {drill_id}: "
                    f"wanted {inc['seq']}, watermark not at {inc['seq'] - 1}"
                )
            self.conn.commit()

    def target_rows(self, drill_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT rid, batch, payload, updated_seq "
                "FROM target_rows WHERE drill_id=? ORDER BY rid",
                (drill_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def record_switch_intent(self, drill_id: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE migrations SET switch_intended=1 WHERE drill_id=?",
                (drill_id,),
            )
            self.conn.commit()

    def expected_target(self, drill_id: str) -> dict[str, dict[str, Any]]:
        """Expected target content = frozen snapshot folded with the changelog.

        This pure function of durable state is the authority used by the
        switch gate: target must be byte-identical to snapshot + recorded
        increments, proving no update was lost, duplicated or rolled back.
        """
        expected: dict[str, dict[str, Any]] = {
            r["rid"]: {
                "batch": r["batch"],
                "payload": r["payload"],
                "updated_seq": 0,
            }
            for r in self.snapshot_rows(drill_id)
        }
        for inc in self.increments(drill_id):
            if inc["op"] in ("upsert", "update"):
                expected[inc["rid"]] = {
                    "batch": inc.get("batch"),
                    "payload": inc.get("payload", ""),
                    "updated_seq": inc["seq"],
                }
            else:
                expected.pop(inc["rid"], None)
        return expected

    def coverage(self, drill_id: str) -> dict[str, Any]:
        """Return target coverage of snapshot + recorded increments (gate)."""
        with self._lock:
            row = self.get_migration_row(drill_id)
            if row is None:
                return {"frozen": False}
            max_seq = self.conn.execute(
                "SELECT COALESCE(MAX(seq),0) AS m FROM increments "
                "WHERE drill_id=?",
                (drill_id,),
            ).fetchone()["m"]
            tcount = self.conn.execute(
                "SELECT COUNT(*) AS c FROM target_rows WHERE drill_id=?",
                (drill_id,),
            ).fetchone()["c"]
            scount = self.conn.execute(
                "SELECT COUNT(*) AS c FROM source_rows WHERE drill_id=?",
                (drill_id,),
            ).fetchone()["c"]
            snap_ids = {
                r["rid"]
                for r in self.conn.execute(
                    "SELECT rid FROM snapshot_rows WHERE drill_id=?", (drill_id,)
                )
            }
        expected = self.expected_target(drill_id)
        actual = {
            r["rid"]: {
                "batch": r["batch"],
                "payload": r["payload"],
                "updated_seq": r["updated_seq"],
            }
            for r in self.target_rows(drill_id)
        }
        content_match = actual == expected
        deleted_by_changelog = {
            inc["rid"]
            for inc in self.increments(drill_id)
            if inc["op"] == "delete"
        }
        required_now = snap_ids - deleted_by_changelog
        return {
            "frozen": True,
            "snapshot_done": bool(row["snapshot_done"]),
            "last_applied_seq": row["last_applied_seq"],
            "max_recorded_seq": max_seq,
            "snapshot_covered": required_now.issubset(actual.keys()),
            "content_match": content_match,
            "all_increments_applied": row["last_applied_seq"] >= max_seq,
            "row_counts_match": tcount == scount == len(expected),
            "target_count": tcount,
            "source_count": scount,
            "expected_count": len(expected),
            "mismatches": self._coverage_mismatches(expected, actual),
            "switch_intended": bool(row["switch_intended"]),
            "read_pointer": row["read_pointer"],
            "switched_at": row["switched_at"],
        }

    @staticmethod
    def _coverage_mismatches(expected: dict, actual: dict) -> list[dict]:
        diffs: list[dict] = []
        for rid in sorted(set(expected) | set(actual)):
            if expected.get(rid) != actual.get(rid):
                diffs.append({"rid": rid, "expected": expected.get(rid),
                              "actual": actual.get(rid)})
        return diffs[:20]

    def atomic_switch(self, drill_id: str) -> dict[str, Any]:
        """Atomically flip the read pointer once coverage is complete."""
        with self._lock:
            row = self.get_migration_row(drill_id)
            if row["read_pointer"] == "target":
                return {
                    "switched": True,
                    "already": True,
                    "coverage": self.coverage(drill_id),
                }
            cov = self.coverage(drill_id)
            ok = (
                cov["snapshot_done"]
                and cov["content_match"]
                and cov["all_increments_applied"]
            )
            if not ok:
                return {"switched": False, "already": False, "coverage": cov}
            cur = self.conn.execute(
                "UPDATE migrations SET read_pointer='target', switched_at=? "
                "WHERE drill_id=? AND read_pointer='source'",
                (time.time(), drill_id),
            )
            if cur.rowcount != 1:
                # Lost an in-process race; the pointer only moves once.
                return {
                    "switched": True,
                    "already": True,
                    "coverage": self.coverage(drill_id),
                }
            self.conn.commit()
            return {
                "switched": True,
                "already": False,
                "coverage": self.coverage(drill_id),
            }


def open_store(db_path: str) -> Store:
    return Store(db_path)
