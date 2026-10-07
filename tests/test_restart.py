"""Real-process crash/restart tests.

Spawns the actual ``python -m app`` server as a subprocess with the default
hard-crash mode (``os._exit(7)``), triggers each interruption point over
HTTP, waits for the process to die, then starts a fresh process over the
same SQLite file and asserts the migration converges to the same outcome.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from http.client import RemoteDisconnected

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SPEC = {
    "name": "硬中断恢复演练",
    "records": [
        {"rid": "B1", "payload": "b-one"},
        {"rid": "B2", "payload": "b-two"},
        {"rid": "B3", "payload": "b-three"},
    ],
    "updates": [
        {"batch": 1, "op": "upsert", "rid": "B2", "payload": "b-two-r1"},
        {"batch": 1, "op": "upsert", "rid": "B4", "payload": "b-four"},
        {"batch": 2, "op": "delete", "rid": "B1", "payload": None},
        {"batch": 2, "op": "upsert", "rid": "B3", "payload": "b-three-r2"},
    ],
}
EXPECTED = {
    "B2": "b-two-r1",
    "B3": "b-three-r2",
    "B4": "b-four",
}


class ConnectionDropped(Exception):
    """The server hard-exited before answering (expected at crash points)."""


def http(method, url, body=None, timeout=5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    except (RemoteDisconnected, ConnectionResetError,
            BrokenPipeError, OSError) as e:
        raise ConnectionDropped(str(e))


def wait_dead(proc, timeout=8.0):
    try:
        code = proc.wait(timeout=timeout)
        return code
    except subprocess.TimeoutExpired:
        proc.kill()
        raise


def wait_up(port, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http("GET", f"http://127.0.0.1:{port}/healthz", timeout=1)
            return
        except Exception:
            time.sleep(0.15)
    raise AssertionError(f"server on {port} never came up")


def wait_final(port, did, timeout=12.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _, s = http("GET", f"http://127.0.0.1:{port}/api/drills/{did}")
            m = s.get("migration")
            if m and m["read_pointer"] == "target":
                return s
        except Exception:
            pass
        time.sleep(0.15)
    raise AssertionError("did not converge to target")


class HardCrashRestartTests(unittest.TestCase):
    def setUp(self):
        self.data = tempfile.mkdtemp()
        self.db = os.path.join(self.data, "archive.db")
        # Use an ephemeral port to avoid conflicts.
        self.port = 18080 + (os.getpid() % 1000)

    def _spawn(self):
        env = dict(os.environ, HOST="127.0.0.1", PORT=str(self.port),
                   DB_PATH=self.db, ARCHIVE_STEP_DELAY="0.25")
        env.pop("ARCHIVE_CRASH_MODE", None)  # real hard exit
        proc = subprocess.Popen(
            [sys.executable, "-m", "app"], cwd=ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        wait_up(self.port)
        return proc

    def _create(self, crash_point=None, did="drill-hard"):
        spec = dict(SPEC)
        if crash_point:
            spec["crash_point"] = crash_point
        try:
            http("POST", f"http://127.0.0.1:{self.port}/api/drills",
                 {"drill_id": did, "spec": spec})
        except ConnectionDropped:
            pass  # after_drill_submit hard-exits before the response

    def _start(self, mid="mig-hard"):
        try:
            http("POST",
                 f"http://127.0.0.1:{self.port}/api/drills/drill-hard/migration",
                 {"migration_id": mid})
        except ConnectionDropped:
            pass  # after_freeze hard-exits before the response

    def _assert_final(self):
        s = wait_final(self.port, "drill-hard")
        self.assertTrue(s["coverage"]["content_match"])
        tgt = {r["rid"]: r["payload"] for r in s["target"]}
        self.assertEqual(tgt, EXPECTED)
        src = {r["rid"]: r["payload"] for r in s["source"]}
        self.assertEqual(src, tgt)
        return s

    def test_restart_achieves_consistency_without_interruption(self):
        proc = self._spawn()
        try:
            self._create()
            self._start("mig-0")
            self._assert_final()
        finally:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5)

    def test_crash_at_every_point_then_reboot_converges(self):
        points = [
            "after_freeze",
            "after_snapshot",
            "after_increment",
            "after_project",
            "after_switch_intent",
            "after_switch",
        ]
        for point in points:
            with self.subTest(crash=point):
                self._run_one(point)

    def test_crash_after_submit_then_start_and_converge(self):
        proc = self._spawn()
        try:
            self._create("after_drill_submit")
            code = wait_dead(proc)
            self.assertEqual(code, 7)
        finally:
            if proc.poll() is None:
                proc.kill()
        proc = self._spawn()
        try:
            self._start("mig-submit")
            self._assert_final()
        finally:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5)

    def _run_one(self, point):
        # Fresh DB per point.
        self.data = tempfile.mkdtemp()
        self.db = os.path.join(self.data, "archive.db")
        proc = self._spawn()
        try:
            self._create(point)
            self._start(f"mig-{point}")
            code = wait_dead(proc)
            self.assertEqual(code, 7, f"expected hard exit at {point}")
        finally:
            if proc.poll() is None:
                proc.kill()
        # Reopen the service on the same durable state and let it recover.
        proc2 = self._spawn()
        try:
            self._assert_final()
            # A further reboot must report the same completed result and not
            # replay any increment.
            before = self._read_coverage()
            proc2.send_signal(signal.SIGINT)
            proc2.wait(timeout=5)
            proc3 = self._spawn()
            try:
                time.sleep(0.6)
                after = self._read_coverage()
                self.assertEqual(after["last_applied_seq"],
                                 before["last_applied_seq"])
                self.assertEqual(after["read_pointer"], "target")
            finally:
                proc3.send_signal(signal.SIGINT)
                proc3.wait(timeout=5)
        finally:
            if proc2.poll() is None:
                proc2.kill()

    def _read_coverage(self):
        _, s = http("GET",
                    f"http://127.0.0.1:{self.port}/api/drills/drill-hard")
        return s["coverage"]


if __name__ == "__main__":
    unittest.main(verbosity=2)
