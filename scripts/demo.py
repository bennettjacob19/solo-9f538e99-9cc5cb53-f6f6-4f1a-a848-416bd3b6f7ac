"""Live drill against a running server: rebind + capacity-shortage scenarios.

Usage:
    EGRESS_A_CAPACITY=20 EGRESS_B_CAPACITY=20 SEND_INTERVAL=0.002 \
        uvicorn app.main:app --port 8000 &
    python scripts/demo.py http://127.0.0.1:8000
"""
from __future__ import annotations

import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
MAC1 = "00:00:00:00:00:11"
MAC2 = "00:00:00:00:00:22"
MAC3 = "00:00:00:00:00:33"
MAC4 = "00:00:00:00:00:44"

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def wait_total(client: httpx.Client, name: str, total: int, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        data = client.get(f"/api/receivers/{name}/frames", params={"limit": 5000}).json()
        if data["total_received"] >= total:
            return data
        time.sleep(0.02)
    return client.get(f"/api/receivers/{name}/frames", params={"limit": 5000}).json()


def pending(client: httpx.Client, name: str) -> int:
    data = client.get("/api/egresses").json()["egresses"]
    return next(e["pending"] for e in data if e["name"] == name)


def binding_of(client: httpx.Client, mac: str) -> str | None:
    for b in client.get("/api/bindings").json()["bindings"]:
        if b["dst_mac"] == mac:
            return b["egress"]
    return None


def main() -> int:
    with httpx.Client(base_url=BASE, timeout=10.0) as c:
        for _ in range(50):
            try:
                if c.get("/api/health").status_code == 200:
                    break
            except httpx.TransportError:
                time.sleep(0.1)
        else:
            print("server did not come up")
            return 1

        print(f"== drill against {BASE} ==")
        caps = {e["name"]: e["capacity"] for e in c.get("/api/egresses").json()["egresses"]}
        print(f"startup capacities: {caps}")
        c.post("/api/receivers/reset")

        print("\n-- scenario 1: rebind moves the destination's backlog to the new egress")
        c.post("/api/egresses/A/pause")
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 8})
        check("8 frames queued on paused A", pending(c, "A") == 8, f"A pending={pending(c, 'A')}")
        r = c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "B"})
        body = r.json()
        check("rebind accepted", r.status_code == 200, f"moved_pending={body.get('moved_pending')}")
        check("all 8 moved", body.get("moved_pending") == 8)
        check("old egress drained", pending(c, "A") == 0)
        recv_b = wait_total(c, "B", 8)
        seqs = [f["seq"] for f in recv_b["frames"]]
        check("B received all 8 in enqueue order", seqs == list(range(1, 9)), f"seqs={seqs}")
        c.post("/api/egresses/A/resume")
        time.sleep(0.3)
        recv_a = c.get("/api/receivers/A/frames").json()
        check("nothing leaked out of A", recv_a["total_received"] == 0)

        print("\n-- scenario 2: rebind refused when the new egress cannot hold the backlog")
        c.post("/api/egresses/A/pause")
        c.post("/api/egresses/B/pause")
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC2, "egress": "B"})
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC2, "count": caps["B"] - 6})
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC3, "egress": "A"})
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC3, "count": 7})
        print(f"  B pending={pending(c, 'B')}/{caps['B']} (free 6), A has 7 frames for {MAC3}")
        r = c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC3, "egress": "B"})
        check("rebind refused with 409", r.status_code == 409, r.json().get("detail", ""))
        check("binding unchanged (still A)", binding_of(c, MAC3) == "A")
        check("queues unchanged", pending(c, "A") == 7 and pending(c, "B") == caps["B"] - 6,
              f"A={pending(c, 'A')} B={pending(c, 'B')}")
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC3, "count": 1})
        check("new frames still enqueue on A", r.status_code == 200 and r.json()["egress"] == "A")
        c.post("/api/egresses/A/resume")
        c.post("/api/egresses/B/resume")
        recv_a = wait_total(c, "A", 8)
        recv_b = wait_total(c, "B", 8 + caps["B"] - 6)
        check("A delivered its 8 in order",
              [f["seq"] for f in recv_a["frames"]] == list(range(1, 9)))
        check("B delivered its own backlog", recv_b["total_received"] == 8 + caps["B"] - 6)

        print("\n-- scenario 3: full egress refuses enqueue")
        c.post("/api/egresses/A/pause")
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC4, "egress": "A"})
        room = caps["A"] - pending(c, "A")
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC4, "count": room})
        check(f"A filled to capacity ({caps['A']})", pending(c, "A") == caps["A"])
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC4, "count": 1})
        check("overflow refused with 409", r.status_code == 409, r.json().get("detail", ""))
        check("queue untouched after refusal", pending(c, "A") == caps["A"])
        c.post("/api/egresses/A/resume")
        recv_a = wait_total(c, "A", 8 + caps["A"])
        check("A drained everything after resume", recv_a["total_received"] == 8 + caps["A"])

        print("\n== summary ==")
        if failures:
            print(f"{len(failures)} check(s) FAILED: {failures}")
            return 1
        print("all checks passed")
        return 0


if __name__ == "__main__":
    sys.exit(main())
