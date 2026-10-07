"""Consistency tests for the snapshot+increment migration.

These run against the real HTTP server (wsgiref on an ephemeral port) and, for
the interruption scenarios, against real subprocesses killed with os._exit and
restarted from the same durable data directory.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_healthy(base: str, timeout: float = 15) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=1) as r:
                if r.status == 200:
                    return
        except Exception as e:  # server may be (re)starting
            last = e
        time.sleep(0.15)
    raise AssertionError(f"service never became healthy: {last}")


def req(base: str, path: str, method: str = "GET", body=None, retries: int = 0):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())
        except (urllib.error.URLError, ConnectionError, OSError):
            if attempt >= retries:
                raise
            attempt += 1
            time.sleep(0.2)


def poll_state(base: str, drill_id: str, want: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = req(base, f"/api/drills/{drill_id}")
        if st["migration"] and st["migration"]["state"] == want:
            return st
        time.sleep(0.15)
    raise AssertionError(f"state never reached {want}; last={st['migration'] and st['migration']['state']}")


def start_server(data_dir: str, port: int) -> subprocess.Popen:
    env = dict(os.environ, DATA_DIR=data_dir, PORT=str(port), WORKER_INTERVAL="0.1")
    return subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def assert_target_matches_source(test: unittest.TestCase, st: dict) -> None:
    test.assertEqual(st["migration"]["state"], "SWITCHED")
    test.assertEqual(st["migration"]["pointer"], "target")
    test.assertEqual(st["increments"]["total"], st["increments"]["applied"])
    test.assertIsNotNone(st["conclusion"])
    test.assertTrue(st["conclusion"]["switched"], st["conclusion"])
    test.assertEqual(st["conclusion"]["divergent_rows"], 0)
    test.assertEqual(st["conclusion"]["duplicate_target_rows"], 0)
    test.assertEqual(st["source_projection"]["count"], st["target_projection"]["count"])


class ServerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = self.tmp.name
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.proc = start_server(self.data_dir, self.port)
        wait_healthy(self.base)

    def tearDown(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.tmp.cleanup()

    def create_and_migrate(self, batches, crash_point=None, migration_id=None,
                           initial=20, page_size=4):
        code, drill = req(self.base, "/api/drills", "POST", {
            "name": f"drill-{self._testMethodName}",
            "initial_records": initial, "page_size": page_size,
            "batches": batches, "crash_point": crash_point,
        })
        self.assertEqual(code, 201, drill)
        did = drill["drill_id"]
        mid = migration_id or f"MIG-{self._testMethodName}-{did[:8]}"
        code, resp = req(self.base, f"/api/drills/{did}/migrate", "POST",
                         {"migration_id": mid}, retries=20)
        self.assertEqual(code, 200, resp)
        return did, mid


class TestMigrationConsistency(ServerTestBase):
    def test_happy_path_snapshot_plus_increments(self):
        # one batch lands mid-snapshot, one during catch-up
        did, mid = self.create_and_migrate(
            [{"count": 3, "after_pages": 1}, {"count": 2, "after_pages": None}]
        )
        st = poll_state(self.base, did, "SWITCHED")
        assert_target_matches_source(self, st)
        # 20 initial + 5 post-boundary writes, 20 distinct rows cycled
        self.assertEqual(st["source_write_counter"], 25)
        self.assertEqual(st["increments"]["total"], 5)
        self.assertEqual(sorted(st["fired_batches"]), [0, 1])
        self.assertEqual(st["migration"]["watermark"], 20)

    def test_replay_same_migration_id_does_not_replay(self):
        did, mid = self.create_and_migrate(
            [{"count": 2, "after_pages": 2}], initial=12, page_size=3
        )
        poll_state(self.base, did, "SWITCHED")
        # retransmit the identical migration id: must be an idempotent no-op
        code, resp = req(self.base, f"/api/drills/{did}/migrate", "POST",
                         {"migration_id": mid})
        self.assertEqual(code, 200, resp)
        self.assertTrue(resp["submit"]["idempotent"])
        _, st = req(self.base, f"/api/drills/{did}")
        self.assertEqual(st["increments"]["total"], 2)  # nothing replayed
        self.assertEqual(st["migration"]["state"], "SWITCHED")

    def test_same_migration_id_other_drill_rejected(self):
        did1, mid = self.create_and_migrate([{"count": 1, "after_pages": 1}])
        poll_state(self.base, did1, "SWITCHED")
        code, drill2 = req(self.base, "/api/drills", "POST", {
            "name": "other", "initial_records": 5, "page_size": 2, "batches": []})
        self.assertEqual(code, 201)
        code, resp = req(self.base, f"/api/drills/{drill2['drill_id']}/migrate",
                         "POST", {"migration_id": mid})
        self.assertEqual(code, 409, resp)  # content/identity change rejected
        self.assertIn("rejected", resp["error"])

    def test_updates_before_freeze_refused(self):
        code, drill = req(self.base, "/api/drills", "POST", {
            "name": "nofreeze", "initial_records": 6, "page_size": 2,
            "batches": [{"count": 1, "after_pages": 0}]})
        self.assertEqual(code, 201)
        did = drill["drill_id"]
        code, resp = req(self.base, f"/api/drills/{did}/batches/fire", "POST",
                         {"batch_no": 0})
        self.assertEqual(code, 409, resp)  # boundary not frozen yet

    def test_manual_batch_fire_is_idempotent(self):
        # 30 rows / 2 per page gives a ~1.5s COPYING window to fire manually.
        did, _ = self.create_and_migrate(
            [{"count": 2, "after_pages": 99}, {"count": 1, "after_pages": 99}],
            initial=30, page_size=2)
        # wait until the boundary is frozen and pages are streaming
        deadline = time.time() + 5
        while time.time() < deadline:
            _, st0 = req(self.base, f"/api/drills/{did}")
            if st0["migration"] and st0["migration"]["state"] in ("FROZEN", "COPYING"):
                break
            time.sleep(0.02)
        code, resp = req(self.base, f"/api/drills/{did}/batches/fire", "POST",
                         {"batch_no": 0})
        self.assertEqual(code, 200, resp)
        self.assertFalse(resp["fire"]["idempotent"])
        code, resp = req(self.base, f"/api/drills/{did}/batches/fire", "POST",
                         {"batch_no": 0})
        self.assertEqual(code, 200, resp)
        self.assertTrue(resp["fire"]["idempotent"])  # duplicate fire, no replay
        st = poll_state(self.base, did, "SWITCHED")
        assert_target_matches_source(self, st)
        self.assertEqual(st["increments"]["total"], 3)  # batch 1 fires in catch-up too

    def test_different_migration_id_same_drill_rejected(self):
        did, mid = self.create_and_migrate([{"count": 1, "after_pages": 1}])
        poll_state(self.base, did, "SWITCHED")
        code, resp = req(self.base, f"/api/drills/{did}/migrate", "POST",
                         {"migration_id": mid + "-CHANGED"})
        self.assertEqual(code, 409, resp)
        self.assertIn("rejected", resp["error"])

    def test_drill_content_is_immutable(self):
        # no mutating endpoint exists for drill content
        did, _ = self.create_and_migrate([], initial=4, page_size=2)
        for method in ("PUT", "PATCH", "DELETE"):
            try:
                urllib.request.urlopen(urllib.request.Request(
                    self.base + f"/api/drills/{did}", method=method, data=b"{}",
                    headers={"Content-Type": "application/json"}))
                self.fail(f"{method} should not be accepted")
            except urllib.error.HTTPError as e:
                self.assertIn(e.code, (404, 405))

    def test_pages_watermark_and_events_visible(self):
        did, _ = self.create_and_migrate(
            [{"count": 4, "after_pages": 1}], initial=16, page_size=4)
        st = poll_state(self.base, did, "SWITCHED")
        self.assertEqual(st["migration"]["pages_total"], 4)
        self.assertEqual(st["migration"]["watermark"], 16)
        kinds = {e["kind"] for e in st["events"]}
        # latest events window should include the decisive milestones
        self.assertIn("POINTER_SWITCHED", kinds)
        self.assertIn("SWITCH_INTENT_WRITTEN", kinds)


def run_crash_case(crash_point: str, batches, initial=16, page_size=4):
    """Create drill against server #1 (which dies at the crash point), then
    restart a brand-new process over the same data dir and require convergence
    to the same completed, consistent result."""
    with tempfile.TemporaryDirectory() as data_dir:
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        proc = start_server(data_dir, port)
        try:
            wait_healthy(base)
            code, drill = req(base, "/api/drills", "POST", {
                "name": f"crash-{crash_point}", "initial_records": initial,
                "page_size": page_size, "batches": batches,
                "crash_point": crash_point})
            assert code == 201, drill
            did = drill["drill_id"]
            mid = f"MIG-CRASH-{crash_point}"
            try:
                req(base, f"/api/drills/{did}/migrate", "POST",
                    {"migration_id": mid})
            except Exception:
                pass  # a killed server may reset the connection
            # wait for the process to actually die from the crash point
            deadline = time.time() + 15
            while time.time() < deadline and proc.poll() is None:
                time.sleep(0.1)
            assert proc.poll() is not None, "server did not crash at the interruption point"
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)

        # restart: recovery must converge purely from persisted state
        proc2 = start_server(data_dir, port)
        try:
            wait_healthy(base)
            # replay the same migration id immediately: still no replay
            code, resp = req(base, f"/api/drills/{did}/migrate", "POST",
                             {"migration_id": mid}, retries=30)
            assert code == 200, resp
            assert resp["submit"]["idempotent"], resp
            st = poll_state(base, did, "SWITCHED", timeout=25)
            assert st["conclusion"]["switched"], st["conclusion"]
            assert st["conclusion"]["divergent_rows"] == 0
            assert st["conclusion"]["duplicate_target_rows"] == 0
            assert st["migration"]["pointer"] == "target"
            assert st["increments"]["total"] == st["increments"]["applied"]
        finally:
            proc2.terminate()
            try:
                proc2.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc2.kill()
        return st


class TestCrashRecovery(unittest.TestCase):
    def test_crash_after_submit(self):
        st = run_crash_case("after_submit",
                            [{"count": 2, "after_pages": None}], initial=8, page_size=3)
        self.assertEqual(st["source_write_counter"], 10)

    def test_crash_after_increment_persist(self):
        st = run_crash_case("after_increment_persist",
                            [{"count": 3, "after_pages": 1},
                             {"count": 2, "after_pages": None}],
                            initial=12, page_size=3)
        self.assertEqual(st["source_write_counter"], 17)
        self.assertEqual(st["increments"]["total"], 5)

    def test_crash_after_switch_intent(self):
        st = run_crash_case("after_switch_intent",
                            [{"count": 2, "after_pages": 1},
                             {"count": 1, "after_pages": None}],
                            initial=8, page_size=2)
        self.assertEqual(st["source_write_counter"], 11)
        self.assertEqual(st["increments"]["total"], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
