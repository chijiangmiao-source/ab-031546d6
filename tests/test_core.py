"""Core consistency tests: crash convergence, exactly-once, idempotency.

With ARCHIVE_CRASH_MODE=exception a firing crash point kills the worker
exactly like a process that lost power (no further steps run); the test then
"reboots" by constructing a fresh Store/Service on the same database file.
"""

import os
import tempfile
import threading
import time
import unittest

os.environ["ARCHIVE_CRASH_MODE"] = "exception"
os.environ["ARCHIVE_STEP_DELAY"] = "0"

from app.migration import (  # noqa: E402
    CRASH_AFTER_FREEZE,
    CRASH_AFTER_INCREMENT,
    CRASH_AFTER_PROJECT,
    CRASH_AFTER_SNAPSHOT,
    CRASH_AFTER_SWITCH,
    CRASH_AFTER_SWITCH_INTENT,
    MigrationError,
    MigrationService,
)
from app.storage import CrashNow, open_store  # noqa: E402

SPEC = {
    "name": "一致性演练",
    "records": [
        {"rid": "RS-1", "payload": "frame-1-original"},
        {"rid": "RS-2", "payload": "frame-2-original"},
        {"rid": "RS-3", "payload": "frame-3-original"},
    ],
    "updates": [
        {"batch": 1, "op": "upsert", "rid": "RS-2", "payload": "frame-2-b1"},
        {"batch": 1, "op": "upsert", "rid": "RS-4", "payload": "frame-4-new"},
        {"batch": 2, "op": "delete", "rid": "RS-3", "payload": None},
        {"batch": 2, "op": "upsert", "rid": "RS-1", "payload": "frame-1-b2"},
    ],
}

FINAL_CONTENT = {
    "RS-1": {"batch": 2, "payload": "frame-1-b2", "updated_seq": 4},
    "RS-2": {"batch": 1, "payload": "frame-2-b1", "updated_seq": 1},
    "RS-4": {"batch": 1, "payload": "frame-4-new", "updated_seq": 2},
}


def make_service(root: str) -> MigrationService:
    store = open_store(os.path.join(root, "archive.db"))
    svc = MigrationService(store)
    return svc


def expected_target(store, drill_id: str) -> dict:
    return store.expected_target(drill_id)


def wait_done(svc: MigrationService, did: str, timeout: float = 8.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = svc.store.get_migration_row(did)
        if row is None:
            time.sleep(0.02)
            continue
        if row["read_pointer"] == "target":
            return svc.store.coverage(did)
        time.sleep(0.02)
    raise AssertionError("migration did not finish in time")


def assert_final_state(testcase: unittest.TestCase, svc: MigrationService,
                       did: str) -> None:
    cov = wait_done(svc, did)
    testcase.assertEqual(cov["read_pointer"], "target")
    testcase.assertTrue(cov["content_match"])
    testcase.assertTrue(cov["all_increments_applied"])
    testcase.assertEqual(cov["last_applied_seq"], 4)
    testcase.assertEqual(cov["max_recorded_seq"], 4)
    actual = {
        r["rid"]: {
            "batch": r["batch"],
            "payload": r["payload"],
            "updated_seq": r["updated_seq"],
        }
        for r in svc.store.target_rows(did)
    }
    testcase.assertEqual(actual, FINAL_CONTENT)
    # Source and target converge too (the old writer never deleted wrongly).
    src = {r["rid"]: r["payload"] for r in svc.store.source_rows(did)}
    tgt = {r["rid"]: r["payload"] for r in svc.store.target_rows(did)}
    testcase.assertEqual(src, tgt)


class HappyPathTests(unittest.TestCase):
    def test_full_migration_without_crash(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), None)
            svc.start(did, "mig-happy")
            assert_final_state(self, svc, did)


class CrashConvergenceTests(unittest.TestCase):
    def _run_with_crash(self, point: str):
        root = tempfile.mkdtemp()
        spec = dict(SPEC, crash_point=point)
        # First boot: create and start; the worker dies at the crash point.
        svc1 = make_service(root)
        if point == "after_drill_submit":
            # The crash fires from the submission path right after the commit.
            did = "drill-after-submit"
            with self.assertRaises(CrashNow):
                svc1.create_drill(spec, did)
            self.assertTrue(os.path.exists(os.path.join(root, "archive.db")))
        else:
            did = svc1.create_drill(spec, None)
            try:
                svc1.start(did, f"mig-{point}")
            except CrashNow:
                # In exception mode the freeze-point crash propagates out of
                # start(); on a real host it would be an immediate process
                # exit instead.
                self.assertEqual(point, CRASH_AFTER_FREEZE)
            self._wait_worker_dead(svc1, did)
        svc1.store.close()

        # Reboot: a new service over the durable state must converge.
        svc2 = make_service(root)
        svc2.recover_on_boot()
        try:
            if svc2.store.get_migration_row(did) is None:
                # Submission crashed before the boundary was frozen; the user
                # now starts the migration against the persisted drill.
                svc2.start(did, f"mig-{point}")
            assert_final_state(self, svc2, did)
        finally:
            svc2.store.close()
        return root, did

    def _wait_worker_dead(self, svc, did, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if did not in svc._active:
                return
            time.sleep(0.02)
        raise AssertionError("worker did not die at crash point")

    def test_crash_after_freeze(self):
        self._run_with_crash(CRASH_AFTER_FREEZE)

    def test_crash_after_snapshot(self):
        self._run_with_crash(CRASH_AFTER_SNAPSHOT)

    def test_crash_after_increment(self):
        self._run_with_crash(CRASH_AFTER_INCREMENT)

    def test_crash_after_project(self):
        self._run_with_crash(CRASH_AFTER_PROJECT)

    def test_crash_after_switch_intent(self):
        self._run_with_crash(CRASH_AFTER_SWITCH_INTENT)

    def test_crash_after_switch(self):
        root, did = self._run_with_crash(CRASH_AFTER_SWITCH)
        # After-switch crash on reboot must not replay anything and must stay
        # switched (the single completed outcome).
        svc3 = make_service(root)
        svc3.recover_on_boot()
        time.sleep(0.3)
        cov = svc3.store.coverage(did)
        self.assertEqual(cov["read_pointer"], "target")
        self.assertEqual(cov["last_applied_seq"], 4)
        svc3.store.close()

    def test_crash_after_drill_submit(self):
        root, did = self._run_with_crash("after_drill_submit")
        svc = make_service(root)
        # Drill definition survived even though the process "died"
        # immediately after submission.
        spec = svc.store.get_drill(did)
        self.assertIsNotNone(spec)
        self.assertEqual(len(spec["records"]), 3)
        svc.store.close()


class IdempotencyTests(unittest.TestCase):
    def test_retransmit_same_migration_id_does_not_replay(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), None)
            r1 = svc.start(did, "mig-same")
            self.assertTrue(r1["created"])
            assert_final_state(self, svc, did)
            seq_after = svc.store.increments(did)[-1]["seq"]
            # Retransmit the identical migration id: no new increments,
            # pointer remains target, no error.
            r2 = svc.start(did, "mig-same")
            self.assertFalse(r2["created"])
            incs = svc.store.increments(did)
            self.assertEqual(incs[-1]["seq"], seq_after)
            self.assertEqual(
                svc.store.get_migration_row(did)["read_pointer"], "target"
            )

    def test_migration_id_conflict_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            d1 = svc.create_drill(dict(SPEC), None)
            d2 = svc.create_drill(dict(SPEC), None)
            svc.start(d1, "mig-x")
            assert_final_state(self, svc, d1)
            with self.assertRaises(MigrationError) as cm:
                svc.start(d2, "mig-x")
            self.assertEqual(cm.exception.status, 409)

    def test_drill_definition_immutable(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), "fixed-id")
            changed = dict(SPEC, records=SPEC["records"] + [
                {"rid": "RS-9", "payload": "tampered"}])
            with self.assertRaises(MigrationError) as cm:
                svc.create_drill(changed, "fixed-id")
            self.assertEqual(cm.exception.code, "drill_immutable")
            # Identical resubmission is accepted idempotently.
            again = svc.create_drill(dict(SPEC), "fixed-id")
            self.assertEqual(again, "fixed-id")

    def test_write_token_dedup(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), None)
            # Freeze first so the extra write becomes a recorded increment,
            # then drive the pipeline manually to avoid timing races.
            svc.store.freeze_boundary(did, "mig-dedup")
            upd = {"op": "upsert", "rid": "RS-7", "batch": 9,
                   "payload": "once"}
            r1 = svc.ingest_write(did, "tok-1", upd)
            r2 = svc.ingest_write(did, "tok-1", dict(upd, payload="twice"))
            self.assertFalse(r1["skipped"])
            self.assertTrue(r2["skipped"])
            svc.spawn_worker(did)
            cov = wait_done(svc, did)
            self.assertTrue(cov["content_match"])
            rows = [i for i in svc.store.increments(did)
                    if i["rid"] == "RS-7"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["payload"], "once")


class GateTests(unittest.TestCase):
    def test_switch_refused_before_full_coverage(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), None)
            svc.store.freeze_boundary(did, "mig-gate")
            # Nothing copied yet: switch must be refused.
            res = svc.store.atomic_switch(did)
            self.assertFalse(res["switched"])
            self.assertEqual(res["coverage"]["read_pointer"], "source")

    def test_double_application_is_impossible(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), None)
            svc.store.freeze_boundary(did, "mig-seq")
            svc.ingest_write(did, "a", {
                "op": "upsert", "rid": "RS-2", "batch": 1,
                "payload": "x"})
            incs = svc.store.pending_increments(did)
            self.assertEqual([i["seq"] for i in incs], [1])
            svc.store.project_increment(did, incs[0])
            # Replaying seq=1 must not be accepted (watermark already at 1).
            with self.assertRaises(RuntimeError):
                svc.store.project_increment(did, incs[0])
            self.assertEqual(
                svc.store.get_migration_row(did)["last_applied_seq"], 1)
            row = [r for r in svc.store.target_rows(did)
                   if r["rid"] == "RS-2"][0]
            self.assertEqual(row["payload"], "x")


class ConcurrentWriteTests(unittest.TestCase):
    def test_concurrent_old_writes_keep_order_and_once(self):
        with tempfile.TemporaryDirectory() as root:
            svc = make_service(root)
            did = svc.create_drill(dict(SPEC), None)
            # Freeze the boundary, let ten old-collector threads record
            # increments concurrently, and only then project + switch.
            svc.store.freeze_boundary(did, "mig-concurrent")

            def write(i):
                svc.ingest_write(did, f"c-{i}", {
                    "op": "upsert", "rid": f"RC-{i}", "batch": 5,
                    "payload": f"p-{i}"})

            threads = [threading.Thread(target=write, args=(i,))
                       for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            svc.spawn_worker(did)
            cov = wait_done(svc, did)
            self.assertTrue(cov["content_match"])
            seqs = [i["seq"] for i in svc.store.increments(did)]
            self.assertEqual(sorted(seqs), seqs)
            self.assertEqual(len(seqs), len(set(seqs)))
            self.assertEqual(len(seqs), 14)  # 4 planned + 10 concurrent
            tgt = {r["rid"] for r in svc.store.target_rows(did)}
            for i in range(10):
                self.assertIn(f"RC-{i}", tgt)


if __name__ == "__main__":
    unittest.main(verbosity=2)
