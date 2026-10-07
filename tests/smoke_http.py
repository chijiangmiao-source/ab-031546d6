#!/usr/bin/env python3
"""HTTP smoke test for the running archive station.

Drives a complete migration through the real API and asserts the switch
conclusion. Exits 0 on success, 1 otherwise. Used by the Compose verify
service (with --network container:<web>) and usable locally:

    BASE_URL=http://127.0.0.1:8080 python3 tests/smoke_http.py
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8080")
TIMEOUT = 15

SPEC = {
    "name": "HTTP 冒烟演练",
    "records": [
        {"rid": "S-1", "payload": "smoke-frame-1"},
        {"rid": "S-2", "payload": "smoke-frame-2"},
        {"rid": "S-3", "payload": "smoke-frame-3"},
    ],
    "updates": [
        {"batch": 1, "op": "upsert", "rid": "S-2", "payload": "smoke-frame-2b"},
        {"batch": 1, "op": "upsert", "rid": "S-4", "payload": "smoke-frame-4"},
        {"batch": 2, "op": "delete", "rid": "S-1", "payload": None},
        {"batch": 2, "op": "upsert", "rid": "S-3", "payload": "smoke-frame-3b"},
    ],
}
EXPECTED_TARGET = {
    "S-2": "smoke-frame-2b",
    "S-3": "smoke-frame-3b",
    "S-4": "smoke-frame-4",
}


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(cond, label):
    print(("  ✓ " if cond else "  ✗ ") + label)
    if not cond:
        raise AssertionError(label)


def main():
    # Unique per run so smoke is safe to repeat against the same container.
    run_id = uuid.uuid4().hex[:8]
    drill_id = f"smoke-{run_id}"
    mig_id = f"smoke-mig-{run_id}"
    print(f"[smoke] base url = {BASE_URL}, drill = {drill_id}")
    # 1) health + static UI
    last_err = None
    for _ in range(30):
        try:
            st, h = call("GET", "/healthz")
            if st == 200:
                break
        except Exception as e:
            last_err = e
        time.sleep(0.5)
    else:
        print(f"[smoke] health never became healthy: {last_err}")
        return 1
    check(h["status"] == "ok", f"健康检查通过（uptime={h['uptime_sec']}s）")
    with urllib.request.urlopen(BASE_URL + "/", timeout=TIMEOUT) as r:
        html = r.read().decode()
    check("遥感档案站" in html, "页面可访问")

    # 2) create drill (real API)
    st, c = call("POST", "/api/drills",
                 {"drill_id": drill_id, "spec": SPEC})
    check(st == 201 and c["drill_id"] == drill_id, "演练已创建")

    # 3) immutability: changed content rejected, identical resubmit ok
    tampered = dict(SPEC, records=SPEC["records"] + [
        {"rid": "EVIL", "payload": "nope"}])
    st, err = call("POST", "/api/drills",
                   {"drill_id": drill_id, "spec": tampered})
    check(st == 409 and err["error"] == "drill_immutable",
          "篡改演练内容被拒绝（409 drill_immutable）")
    st, _ = call("POST", "/api/drills",
                 {"drill_id": drill_id, "spec": SPEC})
    check(st == 201, "相同定义重复提交幂等通过")

    # 4) start migration with a stable id
    st, m = call("POST", f"/api/drills/{drill_id}/migration",
                 {"migration_id": mig_id})
    check(st == 200 and m["created"] is True, "复制边界已冻结并启动迁移")

    # retransmit must not replay
    st, m2 = call("POST", f"/api/drills/{drill_id}/migration",
                  {"migration_id": mig_id})
    check(st == 200 and m2["created"] is False,
          "重传相同 migration_id 不重放")

    # 5) poll until atomic switch
    status = None
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        st, status = call("GET", f"/api/drills/{drill_id}")
        mig = status.get("migration")
        if mig and mig["read_pointer"] == "target":
            break
        time.sleep(0.3)
    cov = status["coverage"]
    check(cov["read_pointer"] == "target", "读取指针已原子切换为 target")
    check(cov["snapshot_done"], "快照水位已置位")
    check(cov["max_recorded_seq"] == 4 and cov["last_applied_seq"] == 4,
          "全部 4 条有序增量恰好一次投影")
    check(cov["content_match"], "目标内容 == 快照 + 全部增量（无丢失/重复/回退）")
    target = {r["rid"]: r["payload"] for r in status["target"]}
    check(target == EXPECTED_TARGET,
          f"目标投影正确：{sorted(target)}")
    source = {r["rid"]: r["payload"] for r in status["source"]}
    check(source == target, "源表与目标表最终一致")

    # 6) late write after cutover is rejected, pointer cannot roll back
    st, err = call("POST", f"/api/drills/{drill_id}/writes", {
        "token": "smoke-late", "op": "upsert", "rid": "S-2",
        "batch": 9, "payload": "rollback-attempt"})
    check(st == 409 and err["error"] == "cutover_complete",
          "切换后旧端写入被拒绝（指针不回退）")
    st, status2 = call("GET", f"/api/drills/{drill_id}")
    check(status2["migration"]["read_pointer"] == "target",
          "切换结论保持稳定")

    print("[smoke] all checks passed ✅")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"[smoke] FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[smoke] ERROR: {e}")
        sys.exit(1)
