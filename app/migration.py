"""Migration orchestration: freeze -> snapshot -> ordered increments -> switch.

The pipeline is driven by a single resumable worker per drill. Every step is
idempotent and persisted, so a hard process exit at any point converges, on
restart, to exactly one of two outcomes:

* ``target``  – the atomic switch happened, source and target agree,
* ``source``  – the switch has not happened; coverage details explain why.

Planned old-collector writes carry stable client tokens, so retransmitting a
migration (or re-running after a crash mid-batch) never replays an update.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Optional

from .storage import (
    ALL_CRASH_POINTS,
    CRASH_AFTER_FREEZE,
    CRASH_AFTER_INCREMENT,
    CRASH_AFTER_PROJECT,
    CRASH_AFTER_SNAPSHOT,
    CRASH_AFTER_SWITCH,
    CRASH_AFTER_SWITCH_INTENT,
    CrashNow,
    Store,
)

STEP_DELAY = float(os.environ.get("ARCHIVE_STEP_DELAY", "0.2"))
# "hard" (default): os._exit like real power loss; "exception": raise through
# so tests can reopen the durable state in-process and verify convergence.
CRASH_MODE = os.environ.get("ARCHIVE_CRASH_MODE", "hard")

EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS fired_crashes (
    drill_id TEXT NOT NULL,
    point TEXT NOT NULL,
    fired_at REAL NOT NULL,
    PRIMARY KEY (drill_id, point)
);
CREATE TABLE IF NOT EXISTS ingested_writes (
    drill_id TEXT NOT NULL,
    token TEXT NOT NULL,
    PRIMARY KEY (drill_id, token)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drill_id TEXT NOT NULL,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL
);
"""


class MigrationError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def validate_spec(spec: Any) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise MigrationError(400, "bad_spec", "spec must be a JSON object")
    name = str(spec.get("name", "未命名演练"))[:80]
    records = spec.get("records", [])
    if not isinstance(records, list) or not records:
        raise MigrationError(400, "bad_spec", "至少需要一条初始记录")
    norm_records = []
    seen: set[str] = set()
    for i, r in enumerate(records):
        if not isinstance(r, dict) or "rid" not in r:
            raise MigrationError(400, "bad_spec", f"记录 #{i} 缺少 rid")
        rid = str(r["rid"])
        if rid in seen:
            raise MigrationError(400, "bad_spec", f"记录 rid 重复: {rid}")
        seen.add(rid)
        norm_records.append({"rid": rid, "payload": str(r.get("payload", ""))})

    updates = spec.get("updates", [])
    if not isinstance(updates, list):
        raise MigrationError(400, "bad_spec", "updates 必须是数组")
    norm_updates = []
    for i, u in enumerate(updates):
        if not isinstance(u, dict):
            raise MigrationError(400, "bad_spec", f"更新 #{i} 格式错误")
        op = u.get("op", "upsert")
        if op not in ("upsert", "update", "delete"):
            raise MigrationError(400, "bad_spec", f"未知操作: {op}")
        batch = u.get("batch", 1)
        try:
            batch = int(batch)
        except (TypeError, ValueError):
            raise MigrationError(400, "bad_spec", f"更新 #{i} 批次不是整数")
        if batch < 1:
            raise MigrationError(400, "bad_spec", f"更新 #{i} 批次必须 >= 1")
        rid = str(u.get("rid", ""))
        if not rid:
            raise MigrationError(400, "bad_spec", f"更新 #{i} 缺少 rid")
        norm_updates.append(
            {
                "batch": batch,
                "op": op,
                "rid": rid,
                "payload": None if op == "delete" else str(u.get("payload", "")),
            }
        )
    norm_updates.sort(key=lambda u: (u["batch"],))

    crash_point = spec.get("crash_point") or None
    if crash_point is not None and crash_point not in ALL_CRASH_POINTS:
        raise MigrationError(400, "bad_spec", f"未知中断点: {crash_point}")

    return {
        "name": name,
        "records": norm_records,
        "updates": norm_updates,
        "crash_point": crash_point,
    }


class MigrationService:
    def __init__(self, store: Store) -> None:
        self.store = store
        self._active: set[str] = set()
        self._active_lock = threading.Lock()
        with store._lock:
            store.conn.executescript(EVENTS_DDL)
            store.conn.commit()

    # ------------------------------------------------------------ utilities
    def log_event(self, drill_id: str, kind: str, message: str) -> None:
        with self.store._lock:
            self.store.conn.execute(
                "INSERT INTO events (drill_id, ts, kind, message) VALUES (?,?,?,?)",
                (drill_id, time.time(), kind, message),
            )
            self.store.conn.commit()

    def events(self, drill_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.conn.execute(
                "SELECT ts, kind, message FROM events WHERE drill_id=? "
                "ORDER BY id DESC LIMIT ?",
                (drill_id, limit),
            ).fetchall()
        return [dict(r) for r in rows][::-1]

    def crash(self, drill_id: str, point: str) -> None:
        """Fire the drill's interruption at ``point`` (at most once ever)."""
        spec = self.store.get_drill(drill_id)
        if not spec or spec.get("crash_point") != point:
            return
        with self.store._lock:
            fired = self.store.conn.execute(
                "SELECT 1 FROM fired_crashes WHERE drill_id=? AND point=?",
                (drill_id, point),
            ).fetchone()
            if fired:
                return
            self.store.conn.execute(
                "INSERT INTO fired_crashes (drill_id, point, fired_at) "
                "VALUES (?,?,?)",
                (drill_id, point, time.time()),
            )
            self.store.conn.execute(
                "INSERT INTO events (drill_id, ts, kind, message) "
                "VALUES (?,?,?,?)",
                (drill_id, time.time(), "crash",
                 f"模拟硬中断：{point}（提交已落盘，进程立即退出）"),
            )
            self.store.conn.commit()
        raise CrashNow(point)

    # ------------------------------------------------------------- creation
    def create_drill(self, spec: dict[str, Any], drill_id: Optional[str]) -> str:
        norm = validate_spec(spec)
        import uuid

        did = drill_id or uuid.uuid4().hex[:12]
        with self.store._lock:
            existing = self.store.get_spec_raw(did)
            if existing is not None:
                if json.loads(existing) != norm:
                    raise MigrationError(
                        409, "drill_immutable",
                        "演练已存在且内容不可修改；重传必须使用完全相同的定义",
                    )
                self.log_event(did, "submit", "重复提交：定义一致，幂等返回")
                return did
            self.store.create_drill(did, norm)
            self.log_event(did, "submit", f"演练已提交（{len(norm['records'])} 条初始记录）")
        if norm.get("crash_point") == "after_drill_submit":
            self.crash(did, "after_drill_submit")
        return did

    # ------------------------------------------------------------- ingestion
    def ingest_write(
        self, drill_id: str, token: str, upd: dict[str, Any]
    ) -> dict[str, Any]:
        """Persist one old-collector write with an idempotency token."""
        from .storage import CutoverComplete

        with self.store._lock:
            try:
                self.store.conn.execute(
                    "INSERT INTO ingested_writes (drill_id, token) VALUES (?,?)",
                    (drill_id, token),
                )
            except sqlite3.IntegrityError:
                return {"skipped": True, "reason": "duplicate_token"}
            try:
                res = self.store.apply_old_write(drill_id, upd)
            except CutoverComplete:
                # Token and write land together or not at all.
                self.store.conn.execute(
                    "DELETE FROM ingested_writes WHERE drill_id=? AND token=?",
                    (drill_id, token),
                )
                self.store.conn.commit()
                raise MigrationError(
                    409, "cutover_complete",
                    "读取指针已切换至 target，旧采集端写入被拒绝（不会回退）",
                )
            except Exception:
                self.store.conn.execute(
                    "DELETE FROM ingested_writes WHERE drill_id=? AND token=?",
                    (drill_id, token),
                )
                self.store.conn.commit()
                raise
            self.store.conn.commit()
            return {"skipped": False, **res}

    def ingest_next_planned_batch(self, drill_id: str) -> Optional[int]:
        """Ingest the first batch with any not-yet-durable writes.

        Completion of a batch is defined per write token, so a crash in the
        middle of a batch resumes the remaining writes instead of skipping
        the batch and silently losing updates.
        """
        spec = self.store.get_drill(drill_id)
        assert spec is not None
        with self.store._lock:
            ingested = {
                r["token"]
                for r in self.store.conn.execute(
                    "SELECT token FROM ingested_writes WHERE drill_id=?",
                    (drill_id,),
                ).fetchall()
            }
        for batch in sorted({u["batch"] for u in spec["updates"]}):
            ups = [u for u in spec["updates"] if u["batch"] == batch]
            tokens = {
                f"planned:{batch}:{idx}:{u['rid']}:{u['op']}"
                for idx, u in enumerate(ups)
            }
            if tokens.issubset(ingested):
                continue
            self._ingest_planned_batch(drill_id, spec, batch)
            return batch
        return None

    def _ingest_planned_batch(
        self, drill_id: str, spec: dict[str, Any], batch: int
    ) -> None:
        ups = [u for u in spec["updates"] if u["batch"] == batch]
        self.log_event(
            drill_id, "ingest",
            f"旧采集端触发批次 {batch}（{len(ups)} 条写入）",
        )
        first_post_freeze = True
        for idx, upd in enumerate(ups):
            token = f"planned:{batch}:{idx}:{upd['rid']}:{upd['op']}"
            res = self.ingest_write(drill_id, token, upd)
            if not res["skipped"] and res.get("frozen") and first_post_freeze:
                first_post_freeze = False
                self.log_event(
                    drill_id, "increment",
                    f"边界后写入已持久化为有序增量 seq={res['seq']}",
                )
                self.crash(drill_id, CRASH_AFTER_INCREMENT)

    # ------------------------------------------------------------- pipeline
    def start(self, drill_id: str, migration_id: str) -> dict[str, Any]:
        spec = self.store.get_drill(drill_id)
        if spec is None:
            raise MigrationError(404, "not_found", "演练不存在")
        if not migration_id or not isinstance(migration_id, str):
            raise MigrationError(400, "bad_request", "需要 migration_id")
        try:
            created = self.store.freeze_boundary(drill_id, migration_id)
        except ValueError as e:
            raise MigrationError(409, "migration_id_conflict", str(e))
        except sqlite3.IntegrityError:
            raise MigrationError(
                409, "already_frozen",
                "该演练的复制边界已冻结，不能以新的 migration_id 再次启动",
            )
        if created:
            self.log_event(
                drill_id, "freeze",
                f"复制边界已冻结（migration_id={migration_id}）",
            )
            self.crash(drill_id, CRASH_AFTER_FREEZE)
        else:
            self.log_event(
                drill_id, "freeze",
                f"重传 migration_id={migration_id}：不重放，继续恢复",
            )
        self.spawn_worker(drill_id)
        row = self.store.get_migration_row(drill_id)
        return {"created": created, "migration_id": row["migration_id"]}

    def spawn_worker(self, drill_id: str) -> None:
        with self._active_lock:
            if drill_id in self._active:
                return
            self._active.add(drill_id)
        t = threading.Thread(
            target=self._worker_guard, args=(drill_id,), daemon=True
        )
        t.start()

    def recover_on_boot(self) -> None:
        for spec in self.store.list_drills():
            row = self.store.get_migration_row(spec["drill_id"])
            if row is None:
                continue
            if row["read_pointer"] == "target":
                self.log_event(
                    spec["drill_id"], "recover",
                    "重启检测到已完成切换，收敛为同一完成结果",
                )
            else:
                self.log_event(
                    spec["drill_id"], "recover",
                    "重启后从未切换状态恢复流水线",
                )
                self.spawn_worker(spec["drill_id"])

    def _worker_guard(self, drill_id: str) -> None:
        try:
            self._run_pipeline(drill_id)
        except CrashNow as c:
            if CRASH_MODE == "exception":
                # Test mode: the durable state survives; the thread simply
                # dies and tests "reboot" by opening a fresh service.
                self.log_event(drill_id, "crash",
                               f"模拟中断（{c}）：工作线程终止，等待重开恢复")
                return
            self.log_event(drill_id, "crash", f"中断触发（{c}），进程退出")
            # Flush WAL metadata then die hard, exactly like power loss.
            os._exit(7)
        except Exception:  # pragma: no cover - logged for operability
            import logging

            logging.exception("migration worker failed: %s", drill_id)
        finally:
            with self._active_lock:
                self._active.discard(drill_id)

    def _drain_pending(self, drill_id: str) -> int:
        applied = 0
        while True:
            pending = self.store.pending_increments(drill_id)
            if not pending:
                return applied
            for inc in pending:
                self.store.project_increment(drill_id, inc)
                applied += 1
                self.log_event(
                    drill_id, "project",
                    f"增量 seq={inc['seq']} {inc['op']} {inc['rid']} 恰好一次投影",
                )
                time.sleep(STEP_DELAY)
                self.crash(drill_id, CRASH_AFTER_PROJECT)

    def _run_pipeline(self, drill_id: str) -> None:
        while True:
            row = self.store.get_migration_row(drill_id)
            if row is None:
                return
            if row["read_pointer"] == "target":
                self.log_event(drill_id, "done", "读取指针已指向 target，迁移完成")
                return

            # 1) frozen snapshot -> target
            if not row["snapshot_done"]:
                n = self.store.copy_snapshot_into_target(drill_id)
                self.store.mark_snapshot_done(drill_id)
                self.log_event(
                    drill_id, "snapshot",
                    f"快照 {n} 条初始记录已复制，快照水位置位",
                )
                time.sleep(STEP_DELAY)
                self.crash(drill_id, CRASH_AFTER_SNAPSHOT)
                continue

            # 2) apply everything already recorded past the boundary
            self._drain_pending(drill_id)

            # 3) the old collector keeps writing: ingest the next batch
            nxt = self.ingest_next_planned_batch(drill_id)
            if nxt is not None:
                time.sleep(STEP_DELAY)
                continue

            # 4) all planned writes durably recorded: intent, final drain
            if not row["switch_intended"]:
                self.store.record_switch_intent(drill_id)
                self.log_event(
                    drill_id, "intent", "切换意图已持久化，等待覆盖校验")
                time.sleep(STEP_DELAY)
                self.crash(drill_id, CRASH_AFTER_SWITCH_INTENT)
                continue

            # picks up writes that landed after the intent (e.g. manual API)
            self._drain_pending(drill_id)

            cov = self.store.coverage(drill_id)
            if not (
                cov["snapshot_done"]
                and cov["content_match"]
                and cov["all_increments_applied"]
            ):
                self.log_event(
                    drill_id, "gate",
                    "覆盖未完成，保持 source 指针："
                    + json.dumps(
                        {k: cov[k] for k in (
                            "snapshot_done", "content_match",
                            "all_increments_applied", "row_counts_match",
                            "last_applied_seq", "max_recorded_seq")},
                        ensure_ascii=False,
                    ),
                )
                time.sleep(STEP_DELAY * 2)
                continue

            res = self.store.atomic_switch(drill_id)
            if res["switched"] and not res["already"]:
                self.log_event(
                    drill_id, "switch",
                    "覆盖快照与全部增量，读取指针原子切换为 target",
                )
                time.sleep(STEP_DELAY)
                self.crash(drill_id, CRASH_AFTER_SWITCH)
                return
            if res["switched"] and res["already"]:
                self.log_event(drill_id, "done", "指针此前已切换，幂等返回")
                return
