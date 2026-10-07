#!/usr/bin/env python3
"""End-to-end HTTP smoke test against a running service.

Exits 0 only when: health is OK, a drill can be created, the migration runs
through snapshot + post-boundary increments, the read pointer switches
atomically, the target is fully consistent with the source, and replaying the
same migration id is an idempotent no-op.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request


def req(base: str, path: str, method: str = "GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(r, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def main(base: str) -> int:
    # 1. health
    for _ in range(50):
        try:
            code, h = req(base, "/health")
            if code == 200 and h["status"] == "ok":
                print(f"[smoke] health ok: {h['service']} db={h['db']}")
                break
        except Exception:
            pass
        time.sleep(0.3)
    else:
        print("[smoke] FAIL: service never became healthy")
        return 1

    # 2. create a drill: 18 initial rows, 3/page, two old-side update batches
    code, d = req(base, "/api/drills", "POST", {
        "name": "http-smoke", "initial_records": 18, "page_size": 3,
        "batches": [{"count": 4, "after_pages": 2},
                    {"count": 3, "after_pages": None}],
    })
    if code != 201:
        print(f"[smoke] FAIL: create -> {code} {d}")
        return 1
    did = d["drill_id"]
    mid = "MIG-SMOKE-REPLAY-001"
    print(f"[smoke] drill created: {did}")

    # 3. submit + migrate
    code, r = req(base, f"/api/drills/{did}/migrate", "POST",
                  {"migration_id": mid})
    if code != 200:
        print(f"[smoke] FAIL: migrate -> {code} {r}")
        return 1

    # 4. poll the state machine to the atomic switch
    st = None
    for _ in range(100):
        code, st = req(base, f"/api/drills/{did}")
        assert code == 200
        m = st["migration"]
        if m and m["state"] == "SWITCHED":
            break
        time.sleep(0.2)
    else:
        print("[smoke] FAIL: migration did not reach SWITCHED")
        return 1

    # 5. consistency assertions
    problems = []
    if st["migration"]["pointer"] != "target":
        problems.append("pointer not on target")
    if st["migration"]["watermark"] != 18:
        problems.append(f"watermark={st['migration']['watermark']} != 18")
    if st["increments"]["total"] != 7 or st["increments"]["applied"] != 7:
        problems.append(f"increments={st['increments']}")
    if st["source_write_counter"] != 25:
        problems.append(f"source counter={st['source_write_counter']} != 25")
    if st["source_projection"]["count"] != st["target_projection"]["count"]:
        problems.append("row counts differ")
    c = st["conclusion"]
    if not c or not c["switched"] or c["divergent_rows"] or c["duplicate_target_rows"]:
        problems.append(f"bad conclusion: {c}")
    if problems:
        print("[smoke] FAIL: " + "; ".join(problems))
        return 1
    print(f"[smoke] switched consistently: {c['verdict']}")

    # 6. replay the identical migration id: must not replay any update
    code, r = req(base, f"/api/drills/{did}/migrate", "POST",
                  {"migration_id": mid})
    if code != 200 or not r["submit"].get("idempotent"):
        print(f"[smoke] FAIL: replay not idempotent: {code} {r}")
        return 1
    code, st2 = req(base, f"/api/drills/{did}")
    if st2["increments"]["total"] != 7 or st2["migration"]["state"] != "SWITCHED":
        print("[smoke] FAIL: replay changed durable state")
        return 1
    print("[smoke] replay of same migration_id ignored (exactly-once preserved)")
    print("[smoke] OK")
    return 0


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080"
    sys.exit(main(base.rstrip("/")))
