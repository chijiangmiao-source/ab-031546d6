"""Migration engine: snapshot boundary, ordered increment journal, exactly-once
projection and atomic read-pointer switch, all driven from durable state.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid

from . import db

# Crash points (at most one per drill, exercised exactly once thanks to
# crash_mark, so the container restart never loops on the same point).
CRASH_AFTER_SUBMIT = "after_submit"
CRASH_AFTER_INCREMENT_PERSIST = "after_increment_persist"
CRASH_AFTER_SWITCH_INTENT = "after_switch_intent"
CRASH_POINTS = {CRASH_AFTER_SUBMIT, CRASH_AFTER_INCREMENT_PERSIST, CRASH_AFTER_SWITCH_INTENT}


class DrillError(Exception):
    """User-facing validation/conflict error (mapped to HTTP 4xx)."""


class MigrationConflict(DrillError):
    pass


def _die(drill_id: str, point: str) -> None:
    """Hard process exit: container orchestrator restarts the service, which
    must converge from durable state on boot."""
    print(f"[crash] drill={drill_id} point={point} -> exiting", flush=True)
    os._exit(1)


def _crash_once(conn, drill_id: str, point: str) -> None:
    """Arm the given crash point at most once per drill. The durable marker
    guarantees the interruption is exercised a single time: after restart the
    same code path is reached again but the marker is present, so convergence
    proceeds instead of looping."""
    already = conn.execute(
        "SELECT 1 FROM crash_mark WHERE drill_id=? AND point=?", (drill_id, point)
    ).fetchone()
    if already:
        db.log_event(conn, drill_id, "CRASH_POINT_SKIPPED", point)
        return
    conn.execute(
        "INSERT INTO crash_mark(drill_id, point) VALUES (?,?)", (drill_id, point)
    )
    db.log_event(conn, drill_id, "CRASH_POINT_ARMED", point)
    conn.commit()
    _die(drill_id, point)


def create_drill(
    name: str,
    initial_records: int,
    page_size: int,
    batches: list[dict],
    crash_point: str | None,
    drill_id: str | None = None,
    migration_id: str | None = None,
) -> str:
    """Create a drill with immutable content plus initial source records.

    If migration_id is supplied the migration intent is persisted in the same
    call — simulating an immediate crash right after submit is then possible.
    """
    if not name or not str(name).strip():
        raise DrillError("name is required")
    initial_records = int(initial_records)
    page_size = int(page_size)
    if initial_records < 1 or initial_records > 10000:
        raise DrillError("initial_records must be between 1 and 10000")
    if page_size < 1:
        raise DrillError("page_size must be >= 1")
    if crash_point and crash_point not in CRASH_POINTS:
        raise DrillError(f"unknown crash_point: {crash_point}")
    norm_batches = []
    for i, b in enumerate(batches):
        count = int(b.get("count", 0))
        if count < 1:
            raise DrillError(f"batch {i} count must be >= 1")
        after_pages = b.get("after_pages")
        if after_pages is not None:
            after_pages = int(after_pages)
            if after_pages < 0:
                raise DrillError(f"batch {i} after_pages must be >= 0")
        norm_batches.append({"count": count, "after_pages": after_pages})

    drill_id = drill_id or str(uuid.uuid4())
    config = {
        "initial_records": initial_records,
        "page_size": page_size,
        "batches": norm_batches,
        "crash_point": crash_point,
    }
    with db.connect() as conn:
        try:
            conn.execute(
                "INSERT INTO drill(id, name, config, page_size, created_at) VALUES (?,?,?,?,?)",
                (drill_id, name.strip(), json.dumps(config), page_size, db.utcnow()),
            )
        except sqlite3.IntegrityError as e:
            raise DrillError("drill id already exists") from e
        # Initial observations already present in the old (source) table.
        rows = [
            (drill_id, f"R{seq:05d}", f"obs-{seq}|v1", 1, seq)
            for seq in range(1, initial_records + 1)
        ]
        conn.executemany(
            "INSERT INTO source_record(drill_id, rid, payload, version, updated_at) "
            "VALUES (?,?,?,?,?)",
            rows,
        )
        conn.execute(
            "INSERT INTO source_counter(drill_id, counter) VALUES (?,?)",
            (drill_id, initial_records),
        )
        db.log_event(conn, drill_id, "DRILL_CREATED", f"{initial_records} initial records")
        if migration_id:
            _insert_intent(conn, drill_id, migration_id)
        db.log_event(conn, drill_id, "MIGRATION_SUBMITTED", migration_id or "")
        want_crash = bool(migration_id) and crash_point == CRASH_AFTER_SUBMIT
        if want_crash:
            _crash_once(conn, drill_id, CRASH_AFTER_SUBMIT)
    return drill_id


def _insert_intent(conn: sqlite3.Connection, drill_id: str, migration_id: str) -> None:
    row = conn.execute(
        "SELECT drill_id FROM migration WHERE migration_id=?", (migration_id,)
    ).fetchone()
    if row is not None:
        if row["drill_id"] != drill_id:
            raise MigrationConflict(
                "migration_id already belongs to a different drill; content changes are rejected"
            )
        raise MigrationConflict("duplicate")  # handled as idempotent success by caller
    row = conn.execute(
        "SELECT migration_id FROM migration WHERE drill_id=?", (drill_id,)
    ).fetchone()
    if row is not None:
        raise MigrationConflict(
            f"drill already has migration {row['migration_id']}; "
            "submitting a different migration_id is rejected"
        )
    conn.execute(
        "INSERT INTO migration(drill_id, migration_id, state, pointer, started_at) "
        "VALUES (?,?,?,?,?)",
        (drill_id, migration_id, db.INTENT, "source", db.utcnow()),
    )


def submit_migration(drill_id: str, migration_id: str) -> dict:
    """Persist the migration intent (idempotent on migration_id)."""
    if not migration_id or not str(migration_id).strip():
        raise DrillError("migration_id is required")
    with db.connect() as conn:
        if db.get_drill(conn, drill_id) is None:
            raise DrillError("drill not found")
        try:
            _insert_intent(conn, drill_id, migration_id.strip())
        except MigrationConflict as e:
            if str(e) == "duplicate":
                row = db.get_migration(conn, drill_id)
                db.log_event(conn, drill_id, "SUBMIT_REPLAY_IGNORED", migration_id)
                return {"idempotent": True, "state": row["state"], "migration_id": migration_id}
            raise
        db.log_event(conn, drill_id, "MIGRATION_SUBMITTED", migration_id)
        config = db.get_config(conn, drill_id)
        if config.get("crash_point") == CRASH_AFTER_SUBMIT:
            _crash_once(conn, drill_id, CRASH_AFTER_SUBMIT)
    return {"idempotent": False, "state": db.INTENT, "migration_id": migration_id}


# --------------------------------------------------------------------------- #
# Old-side (source) updates, journaled as ordered post-boundary increments
# --------------------------------------------------------------------------- #

def fire_batch(drill_id: str, batch_no: int) -> dict:
    """Persist one old-side update batch as ordered increments.

    Safe to call more than once for the same (drill, batch_no): the fired_batch
    dedup table makes retries return the same result without replaying writes.
    """
    # Serialize with the convergence worker: reading the counter, journaling
    # increments and recording the fired batch must be one atomic step.
    with _worker_lock, db.connect() as conn:
        config = db.get_config(conn, drill_id)
        mig = db.get_migration(conn, drill_id)
        if mig is None:
            raise MigrationConflict("migration not started")
        if mig["state"] == db.INTENT:
            raise MigrationConflict("replication boundary not frozen yet; updates are refused")
        if mig["state"] in (db.SWITCH_PENDING, db.SWITCHED):
            raise MigrationConflict("cutover committed; no further source updates accepted")
        batches = config["batches"]
        if batch_no < 0 or batch_no >= len(batches):
            raise DrillError("unknown batch")
        already = conn.execute(
            "SELECT 1 FROM fired_batch WHERE drill_id=? AND batch_no=?",
            (drill_id, batch_no),
        ).fetchone()
        if already:
            return {"idempotent": True, "batch_no": batch_no}
        _persist_batch(conn, drill_id, config, batch_no)
        if config.get("crash_point") == CRASH_AFTER_INCREMENT_PERSIST:
            _crash_once(conn, drill_id, CRASH_AFTER_INCREMENT_PERSIST)
    return {"idempotent": False, "batch_no": batch_no}


def _persist_batch(conn, drill_id: str, config: dict, batch_no: int) -> None:
    """The whole batch (source writes + ordered increment journal) commits
    atomically: either the increments are durable or nothing happened."""
    spec = config["batches"][batch_no]
    count = spec["count"]
    row = conn.execute(
        "SELECT counter FROM source_counter WHERE drill_id=?", (drill_id,)
    ).fetchone()
    counter = row["counter"]
    inc_rows = []
    src_rows = []
    for k in range(1, count + 1):
        seq = counter + k
        # Cycle the old records so some are updates and some interleave; every
        # write gets a strictly monotone sequence number.
        rid = f"R{((seq - 1) % config['initial_records']) + 1:05d}"
        version = seq  # strictly growing version
        payload = f"obs-{rid}|v{version}"
        src_rows.append((payload, version, seq, drill_id, rid))
        inc_rows.append((drill_id, seq, rid, payload, version, 0))
    conn.executemany(
        "UPDATE source_record SET payload=?, version=?, updated_at=? "
        "WHERE drill_id=? AND rid=?",
        src_rows,
    )
    conn.execute(
        "UPDATE source_counter SET counter=? WHERE drill_id=?",
        (counter + count, drill_id),
    )
    conn.executemany(
        "INSERT INTO increment(drill_id, seq, rid, payload, version, applied) "
        "VALUES (?,?,?,?,?,?)",
        inc_rows,
    )
    conn.execute(
        "INSERT INTO fired_batch(drill_id, batch_no) VALUES (?,?)",
        (drill_id, batch_no),
    )
    db.log_event(conn, drill_id, "BATCH_PERSISTED", f"#{batch_no} seqs {counter+1}..{counter+count}")


# --------------------------------------------------------------------------- #
# State machine advancement
# --------------------------------------------------------------------------- #

_worker_lock = threading.RLock()


def _maybe_fire_scheduled(conn, drill_id: str, config: dict, mig) -> bool:
    """Persist due scheduled batches inside the current transaction.

    - while copying: a batch is due once pages_done >= after_pages
    - once catching up: every not-yet-fired batch is due (including a batch
      scheduled after more pages than the snapshot has)
    A mid-snapshot batch crash point interrupts here; after restart the
    durable marker suppresses a second interruption.
    """
    if mig["state"] in (db.INTENT, db.SWITCHED, db.SWITCH_PENDING):
        return False
    pages_done = conn.execute(
        "SELECT COUNT(*) c FROM snap_page WHERE drill_id=?", (drill_id,)
    ).fetchone()["c"]
    catching_up = mig["state"] == db.CATCHING_UP
    fired_any = False
    for i, spec in enumerate(config["batches"]):
        already = conn.execute(
            "SELECT 1 FROM fired_batch WHERE drill_id=? AND batch_no=?", (drill_id, i)
        ).fetchone()
        if already:
            continue
        after = spec.get("after_pages")
        if not catching_up and after is not None and pages_done < after:
            continue
        if not catching_up and after is None:
            continue  # batches with no trigger fire in the catch-up phase
        _persist_batch(conn, drill_id, config, i)
        fired_any = True
        if config.get("crash_point") == CRASH_AFTER_INCREMENT_PERSIST:
            _crash_once(conn, drill_id, CRASH_AFTER_INCREMENT_PERSIST)
    return fired_any


def _freeze_boundary(conn, drill_id: str) -> None:
    """Freeze the replication boundary: record the watermark and take an
    immutable image of every source row at or below it. Later old-side writes
    update source_record but can never mutate this snapshot."""
    row = conn.execute(
        "SELECT counter FROM source_counter WHERE drill_id=?", (drill_id,)
    ).fetchone()
    watermark = row["counter"]
    conn.execute(
        "INSERT INTO snap_row(drill_id, rid, payload, version, updated_at) "
        "SELECT drill_id, rid, payload, version, updated_at FROM source_record "
        "WHERE drill_id=? AND updated_at<=?",
        (drill_id, watermark),
    )
    conn.execute(
        "UPDATE migration SET state=?, watermark=? WHERE drill_id=?",
        (db.FROZEN, watermark, drill_id),
    )
    db.log_event(conn, drill_id, "BOUNDARY_FROZEN", f"watermark={watermark}")


def _copy_snapshot_page(conn, drill_id: str, config: dict, mig) -> None:
    """Copy exactly one snapshot page per tick from the immutable frozen
    image, so old-side batches scheduled mid-snapshot are observable between
    pages (and survive a crash on any page)."""
    watermark = mig["watermark"]
    page_size = config["page_size"]
    pages_done = conn.execute(
        "SELECT COUNT(*) c FROM snap_page WHERE drill_id=?", (drill_id,)
    ).fetchone()["c"]
    last = conn.execute(
        "SELECT last_rid FROM snap_page WHERE drill_id=? ORDER BY page_no DESC LIMIT 1",
        (drill_id,),
    ).fetchone()
    after_rid = last["last_rid"] if last else ""
    if after_rid:
        rows = conn.execute(
            "SELECT rid, payload, version, updated_at FROM snap_row "
            "WHERE drill_id=? AND rid>? ORDER BY rid LIMIT ?",
            (drill_id, after_rid, page_size),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT rid, payload, version, updated_at FROM snap_row "
            "WHERE drill_id=? ORDER BY rid LIMIT ?",
            (drill_id, page_size),
        ).fetchall()
    if not rows:
        conn.execute(
            "UPDATE migration SET state=?, pages_total=? WHERE drill_id=?",
            (db.CATCHING_UP, pages_done, drill_id),
        )
        db.log_event(conn, drill_id, "SNAPSHOT_DONE", f"{pages_done} pages")
        return
    # Streaming the snapshot: leave FROZEN into COPYING after the first page.
    if pages_done > 0:
        conn.execute(
            "UPDATE migration SET state=? WHERE drill_id=?", (db.COPYING, drill_id)
        )
    next_last = rows[-1]["rid"]
    for r in rows:
        conn.execute(
            "INSERT INTO target_record(drill_id, rid, payload, version, updated_at, seq) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(drill_id, rid) DO UPDATE SET "
            "payload=excluded.payload, version=excluded.version, "
            "updated_at=excluded.updated_at, seq=excluded.seq",
            (drill_id, r["rid"], r["payload"], r["version"], r["updated_at"], r["updated_at"]),
        )
    conn.execute(
        "INSERT INTO snap_page(drill_id, page_no, last_rid) VALUES (?,?,?)",
        (drill_id, pages_done, next_last),
    )
    db.log_event(
        conn, drill_id, "SNAPSHOT_PAGE", f"page={pages_done} rows={len(rows)} <= {watermark}"
    )


def _apply_increments(conn, drill_id: str) -> bool:
    """Project unapplied increments in strict seq order. Returns True when all
    recorded increments are applied. Projection + applied-flag share one
    transaction per seq, so each increment is projected exactly once even
    across restarts (the UPSERT itself is also idempotent)."""
    done_all = False
    for _ in range(50):
        row = conn.execute(
            "SELECT * FROM increment WHERE drill_id=? AND applied=0 ORDER BY seq LIMIT 1",
            (drill_id,),
        ).fetchone()
        if row is None:
            done_all = True
            break
        conn.execute(
            "INSERT INTO target_record(drill_id, rid, payload, version, updated_at, seq) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(drill_id, rid) DO UPDATE SET "
            "payload=excluded.payload, version=excluded.version, "
            "updated_at=excluded.updated_at, seq=excluded.seq",
            # the increment's ordering seq is also its source write timestamp
            (drill_id, row["rid"], row["payload"], row["version"], row["seq"], row["seq"]),
        )
        conn.execute(
            "UPDATE increment SET applied=1 WHERE drill_id=? AND seq=?",
            (drill_id, row["seq"]),
        )
        db.log_event(conn, drill_id, "INCREMENT_APPLIED", f"seq={row['seq']} rid={row['rid']}")
    return done_all


def _verify_coverage(conn, drill_id: str, mig) -> tuple[bool, str]:
    """Gate for the switch: target must cover the full snapshot plus every
    recorded increment (no loss, no rollback)."""
    if mig["pages_total"] is None:
        return False, "snapshot incomplete"
    pages = conn.execute(
        "SELECT COUNT(*) c FROM snap_page WHERE drill_id=?", (drill_id,)
    ).fetchone()["c"]
    if pages != mig["pages_total"] or pages == 0:
        return False, f"snapshot pages {pages}/{mig['pages_total']}"
    unapplied = conn.execute(
        "SELECT COUNT(*) c FROM increment WHERE drill_id=? AND applied=0", (drill_id,)
    ).fetchone()["c"]
    if unapplied:
        return False, f"{unapplied} increments not applied"
    # Missing rows or any target projection older than the source = uncovered.
    gap = conn.execute(
        "SELECT COUNT(*) c FROM source_record s LEFT JOIN target_record t "
        "ON t.drill_id=s.drill_id AND t.rid=s.rid "
        "WHERE s.drill_id=? AND (t.rid IS NULL OR t.seq<>s.updated_at "
        "OR t.version<>s.version OR t.payload<>s.payload)",
        (drill_id,),
    ).fetchone()["c"]
    if gap:
        return False, f"{gap} rows diverge between source and target"
    return True, "covered"


def advance(drill_id: str) -> None:
    """Drive one migration forward from whatever durable state it is in."""
    with _worker_lock:
        while True:
            with db.connect() as conn:
                mig = db.get_migration(conn, drill_id)
                if mig is None:
                    return
                config = db.get_config(conn, drill_id)
                state = mig["state"]

                if state == db.INTENT:
                    _freeze_boundary(conn, drill_id)
                    conn.commit()
                    continue

                if state in (db.FROZEN, db.COPYING):
                    _copy_snapshot_page(conn, drill_id, config, mig)
                    new_state = conn.execute(
                        "SELECT state FROM migration WHERE drill_id=?", (drill_id,)
                    ).fetchone()["state"]
                    _maybe_fire_scheduled(conn, drill_id, config,
                                          db.get_migration(conn, drill_id))
                    conn.commit()
                    if new_state != db.CATCHING_UP:
                        return  # tick again later (keeps page streaming visible)
                    # snapshot finished this tick: enter catch-up immediately
                    continue

                if state == db.CATCHING_UP:
                    # Batches scheduled for the catch-up phase must land first.
                    before = conn.execute(
                        "SELECT COUNT(*) c FROM fired_batch WHERE drill_id=?", (drill_id,)
                    ).fetchone()["c"]
                    _maybe_fire_scheduled(conn, drill_id, config, mig)  # may hard-exit
                    after = conn.execute(
                        "SELECT COUNT(*) c FROM fired_batch WHERE drill_id=?", (drill_id,)
                    ).fetchone()["c"]
                    all_done = _apply_increments(conn, drill_id)
                    conn.commit()
                    if after > before or not all_done:
                        continue  # more batches may still appear / more seqs to apply
                    # Coverage gate, then durable switch intent.
                    ok, reason = _verify_coverage(conn, drill_id, mig)
                    if not ok:
                        raise RuntimeError(f"coverage failed at switch: {reason}")
                    conn.execute(
                        "UPDATE migration SET state=? WHERE drill_id=?",
                        (db.SWITCH_PENDING, drill_id),
                    )
                    db.log_event(conn, drill_id, "SWITCH_INTENT_WRITTEN", reason)
                    if config.get("crash_point") == CRASH_AFTER_SWITCH_INTENT:
                        _crash_once(conn, drill_id, CRASH_AFTER_SWITCH_INTENT)
                    conn.commit()
                    continue

                if state == db.SWITCH_PENDING:
                    # Re-verify inside the same transaction that moves the
                    # pointer: coverage check and pointer flip are atomic.
                    ok, reason = _verify_coverage(conn, drill_id, mig)
                    if not ok:
                        raise RuntimeError(f"coverage failed at switch: {reason}")
                    conn.execute(
                        "UPDATE migration SET state=?, pointer='target', switched_at=? "
                        "WHERE drill_id=?",
                        (db.SWITCHED, db.utcnow(), drill_id),
                    )
                    db.log_event(conn, drill_id, "POINTER_SWITCHED", "read pointer -> target")
                    conn.commit()
                    return

                # SWITCHED or anything unknown: convergence reached.
                return


def advance_all() -> None:
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT drill_id FROM migration WHERE state<>?", (db.SWITCHED,)
        ).fetchall()
    for r in rows:
        advance(r["drill_id"])


_worker_started = False


def start_worker(interval: float = 0.25) -> None:
    """Background convergence loop. Also the crash-recovery path: on process
    restart every unfinished migration is resumed purely from durable state."""
    global _worker_started
    if _worker_started:
        return
    _worker_started = True

    def loop() -> None:
        # Give the HTTP server a moment to come up after a crash restart.
        time.sleep(0.2)
        while True:
            try:
                advance_all()
            except Exception as e:  # never let the convergence loop die
                print(f"[worker] error: {e!r}", flush=True)
            time.sleep(interval)

    t = threading.Thread(target=loop, name="migration-worker", daemon=True)
    t.start()
