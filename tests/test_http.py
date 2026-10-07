"""End-to-end HTTP API tests against the real stdlib server (in-process)."""

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

os.environ["ARCHIVE_CRASH_MODE"] = "exception"
os.environ["ARCHIVE_STEP_DELAY"] = "0"

from app.server import make_handler  # noqa: E402
from app.migration import MigrationService  # noqa: E402
from app.storage import open_store  # noqa: E402

SPEC = {
    "name": "HTTP 演练",
    "records": [
        {"rid": "A1", "payload": "a-one"},
        {"rid": "A2", "payload": "a-two"},
    ],
    "updates": [
        {"batch": 1, "op": "upsert", "rid": "A2", "payload": "a-two-v2"},
        {"batch": 1, "op": "delete", "rid": "A1", "payload": None},
        {"batch": 2, "op": "upsert", "rid": "A3", "payload": "a-three"},
    ],
}


class HttpTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        store = open_store(os.path.join(self.tmp.name, "archive.db"))
        self.svc = MigrationService(store)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.svc))
        self.port = self.httpd.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.svc.store.close()
        self.tmp.cleanup()

    def req(self, method, path, body=None, expect=None):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                payload = json.loads(resp.read())
                if expect is not None:
                    self.assertEqual(resp.status, expect)
                return resp.status, payload
        except urllib.error.HTTPError as e:
            payload = json.loads(e.read())
            if expect is not None:
                self.assertEqual(e.code, expect, payload)
            return e.code, payload

    def wait_status(self, did, timeout=8.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, s = self.req("GET", f"/api/drills/{did}")
            m = s["migration"]
            if m and m["read_pointer"] == "target":
                return s
            time.sleep(0.05)
        raise AssertionError("never switched")


class HttpApiTests(HttpTestBase):
    def test_health_and_index(self):
        st, h = self.req("GET", "/healthz")
        self.assertEqual(st, 200)
        self.assertEqual(h["status"], "ok")
        with urllib.request.urlopen(self.base + "/", timeout=5) as r:
            html = r.read().decode()
        self.assertIn("遥感档案站", html)
        with urllib.request.urlopen(self.base + "/static/app.js") as r:
            self.assertEqual(r.status, 200)

    def test_full_flow_via_http(self):
        _, c = self.req("POST", "/api/drills", SPEC, expect=201)
        did = c["drill_id"]
        _, m = self.req("POST", f"/api/drills/{did}/migration",
                        {"migration_id": "mig-http-1"}, expect=200)
        self.assertTrue(m["created"])
        s = self.wait_status(did)
        self.assertTrue(s["coverage"]["content_match"])
        # Final target: A1 deleted, A2 updated, A3 inserted.
        tgt = {r["rid"]: r["payload"] for r in s["target"]}
        self.assertEqual(tgt, {"A2": "a-two-v2", "A3": "a-three"})

    def test_immutable_drill_rejected_over_http(self):
        _, c = self.req("POST", "/api/drills",
                        {"drill_id": "fixed", "spec": SPEC}, expect=201)
        self.assertEqual(c["drill_id"], "fixed")
        tampered = dict(SPEC, records=SPEC["records"] + [
            {"rid": "X9", "payload": "hax"}])
        st, err = self.req("POST", "/api/drills",
                           {"drill_id": "fixed", "spec": tampered}, expect=409)
        self.assertEqual(err["error"], "drill_immutable")
        # Identical resubmit is fine.
        self.req("POST", "/api/drills",
                 {"drill_id": "fixed", "spec": SPEC}, expect=201)

    def test_retransmit_migration_id_no_replay(self):
        _, c = self.req("POST", "/api/drills", SPEC)
        did = c["drill_id"]
        self.req("POST", f"/api/drills/{did}/migration",
                 {"migration_id": "dup-1"})
        self.wait_status(did)
        st, m2 = self.req("POST", f"/api/drills/{did}/migration",
                          {"migration_id": "dup-1"}, expect=200)
        self.assertFalse(m2["created"])
        _, s = self.req("GET", f"/api/drills/{did}")
        self.assertEqual(s["migration"]["read_pointer"], "target")
        self.assertEqual(s["coverage"]["last_applied_seq"],
                         s["coverage"]["max_recorded_seq"])

    def test_late_old_write_after_cutover_rejected(self):
        _, c = self.req("POST", "/api/drills", SPEC)
        did = c["drill_id"]
        self.req("POST", f"/api/drills/{did}/migration",
                 {"migration_id": "late-1"})
        self.wait_status(did)
        st, err = self.req("POST", f"/api/drills/{did}/writes", {
            "token": "late-tok", "op": "upsert", "rid": "A2",
            "batch": 9, "payload": "rollback?"}, expect=409)
        self.assertEqual(err["error"], "cutover_complete")
        _, s = self.req("GET", f"/api/drills/{did}")
        tgt = {r["rid"]: r["payload"] for r in s["target"]}
        self.assertEqual(tgt["A2"], "a-two-v2")

    def test_bad_spec_rejected(self):
        st, err = self.req("POST", "/api/drills", {"name": "x", "records": []},
                           expect=400)
        self.assertEqual(err["error"], "bad_spec")


if __name__ == "__main__":
    unittest.main(verbosity=2)
