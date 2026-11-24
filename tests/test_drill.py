"""End-to-end tests for the dual-egress VXLAN drill.

API-level tests run through FastAPI's TestClient (real lifespan, real UDP on
ephemeral loopback ports). Concurrency tests drive the Forwarder directly
inside one event loop.
"""
from __future__ import annotations

import asyncio
import time

from fastapi.testclient import TestClient

from app.config import Settings
from app.forwarder import Forwarder
from app.main import create_app

MAC1 = "00:00:00:00:00:11"
MAC2 = "00:00:00:00:00:22"
MAC3 = "00:00:00:00:00:33"


def make_settings(**overrides) -> Settings:
    base = dict(
        receiver_host="127.0.0.1",
        receiver_a_port=0,  # ephemeral
        receiver_b_port=0,
        egress_a_capacity=10,
        egress_b_capacity=10,
        send_interval=0.0,
    )
    base.update(overrides)
    return Settings(**base)


def wait_until(predicate, timeout=5.0, interval=0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def received(client: TestClient, name: str) -> dict:
    return client.get(f"/api/receivers/{name}/frames", params={"limit": 5000}).json()


def egress_state(client: TestClient, name: str) -> dict:
    data = client.get("/api/egresses").json()["egresses"]
    return next(e for e in data if e["name"] == name)


# --------------------------------------------------------------------------
# API-level scenarios
# --------------------------------------------------------------------------

def test_bind_inject_receive_on_both_egresses():
    with TestClient(create_app(make_settings())) as c:
        assert c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"}).json()["changed"]
        assert c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC2, "egress": "B"}).json()["changed"]

        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 5})
        assert r.status_code == 200 and r.json()["enqueued"] == 5
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC2, "count": 3})
        assert r.status_code == 200 and r.json()["enqueued"] == 3

        assert wait_until(lambda: received(c, "A")["total_received"] == 5)
        assert wait_until(lambda: received(c, "B")["total_received"] == 3)

        frames_a = received(c, "A")["frames"]
        assert [f["seq"] for f in frames_a] == [1, 2, 3, 4, 5]
        assert all(f["vni"] == 100 and f["dst_mac"] == MAC1 for f in frames_a)
        assert [f["seq"] for f in received(c, "B")["frames"]] == [1, 2, 3]


def test_pause_blocks_only_its_own_egress():
    with TestClient(create_app(make_settings())) as c:
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC2, "egress": "B"})

        paused = c.post("/api/egresses/A/pause").json()
        assert paused["paused"] is True

        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 4})
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC2, "count": 2})

        # B keeps sending while A is paused.
        assert wait_until(lambda: received(c, "B")["total_received"] == 2)
        time.sleep(0.2)
        assert received(c, "A")["total_received"] == 0
        assert egress_state(c, "A")["pending"] == 4

        c.post("/api/egresses/A/resume")
        assert wait_until(lambda: received(c, "A")["total_received"] == 4)
        assert [f["seq"] for f in received(c, "A")["frames"]] == [1, 2, 3, 4]


def test_rebind_moves_pending_frames_to_new_egress():
    with TestClient(create_app(make_settings())) as c:
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})
        c.post("/api/egresses/A/pause")
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 7})
        assert egress_state(c, "A")["pending"] == 7

        r = c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "B"})
        assert r.status_code == 200
        body = r.json()
        assert body["changed"] is True
        assert body["previous_egress"] == "A"
        assert body["moved_pending"] == 7

        # Backlog left A without being sent there; B delivers it in order.
        assert egress_state(c, "A")["pending"] == 0
        assert wait_until(lambda: received(c, "B")["total_received"] == 7)
        assert [f["seq"] for f in received(c, "B")["frames"]] == [1, 2, 3, 4, 5, 6, 7]

        # Even after A resumes, nothing leaks out of the old egress.
        c.post("/api/egresses/A/resume")
        time.sleep(0.2)
        assert received(c, "A")["total_received"] == 0


def test_rebind_to_same_egress_is_noop():
    with TestClient(create_app(make_settings())) as c:
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})
        r = c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})
        assert r.status_code == 200
        assert r.json()["changed"] is False
        assert r.json()["moved_pending"] == 0


def test_rebind_rejected_when_target_cannot_hold_all_pending():
    settings = make_settings(egress_a_capacity=10, egress_b_capacity=5)
    with TestClient(create_app(settings)) as c:
        c.post("/api/egresses/A/pause")
        c.post("/api/egresses/B/pause")
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 7})
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC2, "egress": "B"})
        c.post("/api/frames", json={"vni": 100, "dst_mac": MAC2, "count": 4})
        # B holds 4/5 -> only 1 free slot, cannot take MAC1's 7 frames.
        assert egress_state(c, "B")["pending"] == 4

        r = c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "B"})
        assert r.status_code == 409
        assert "cannot hold 7 pending frame(s)" in r.json()["detail"]

        # Binding and both queues are unchanged.
        bindings = c.get("/api/bindings").json()["bindings"]
        assert {b["dst_mac"]: b["egress"] for b in bindings} == {MAC1: "A", MAC2: "B"}
        assert egress_state(c, "A")["pending"] == 7
        assert egress_state(c, "B")["pending"] == 4

        # New frames for MAC1 still go to A (room for 3 more there).
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 1})
        assert r.status_code == 200 and r.json()["egress"] == "A"

        c.post("/api/egresses/A/resume")
        c.post("/api/egresses/B/resume")
        assert wait_until(lambda: received(c, "A")["total_received"] == 8)
        assert wait_until(lambda: received(c, "B")["total_received"] == 4)
        assert [f["seq"] for f in received(c, "A")["frames"]] == list(range(1, 9))


def test_enqueue_rejected_when_egress_full():
    settings = make_settings(egress_a_capacity=3)
    with TestClient(create_app(settings)) as c:
        c.post("/api/egresses/A/pause")
        c.post("/api/bindings", json={"vni": 100, "dst_mac": MAC1, "egress": "A"})

        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 3})
        assert r.status_code == 200 and r.json()["egress_pending"] == 3

        # One more frame does not fit...
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 1})
        assert r.status_code == 409 and "full" in r.json()["detail"]
        # ...and an oversized batch is refused atomically (nothing enqueued).
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC1, "count": 2})
        assert r.status_code == 409
        assert egress_state(c, "A")["pending"] == 3

        c.post("/api/egresses/A/resume")
        assert wait_until(lambda: received(c, "A")["total_received"] == 3)
        assert [f["seq"] for f in received(c, "A")["frames"]] == [1, 2, 3]


def test_inject_without_binding_rejected():
    with TestClient(create_app(make_settings())) as c:
        r = c.post("/api/frames", json={"vni": 100, "dst_mac": MAC3, "count": 1})
        assert r.status_code == 409
        assert "no binding" in r.json()["detail"]


def test_unknown_egress_and_receiver_404():
    with TestClient(create_app(make_settings())) as c:
        assert c.post("/api/egresses/C/pause").status_code == 404
        assert c.get("/api/receivers/C/frames").status_code == 404


# --------------------------------------------------------------------------
# Concurrency scenarios (direct Forwarder use inside one event loop)
# --------------------------------------------------------------------------

async def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


def test_rebind_while_injecting_paused_old_egress():
    """Inject and rebind race: every frame lands on B exactly once, in order."""
    asyncio.run(_rebind_while_injecting())


async def _rebind_while_injecting():
    settings = make_settings(
        egress_a_capacity=400, egress_b_capacity=400, send_interval=0.001
    )
    fwd = Forwarder(settings)
    await fwd.start()
    try:
        vni, mac = 200, "00:00:00:00:00:99"
        await fwd.bind(vni, mac, "A")
        fwd.pause("A")  # frames pile up on the old egress
        total = 300

        async def producer():
            for _ in range(total):
                await fwd.inject(vni, mac, "02:00:00:00:00:01", 1, None, "race")
                await asyncio.sleep(0)

        prod = asyncio.create_task(producer())
        await asyncio.sleep(0.05)  # let a backlog build on A
        result = await fwd.bind(vni, mac, "B")  # rebind mid-stream
        assert result["moved_pending"] > 0
        await prod

        assert await _wait_for(lambda: fwd.receivers["B"].total == total)
        assert fwd.receivers["A"].total == 0
        seqs = [f["seq"] for f in fwd.receivers["B"].frames]
        assert seqs == list(range(1, total + 1))
    finally:
        await fwd.stop()


def test_rebind_while_old_egress_is_sending():
    """Live rebind: frames split across A and B, each sent exactly once."""
    asyncio.run(_rebind_while_sending())


async def _rebind_while_sending():
    settings = make_settings(
        egress_a_capacity=2000, egress_b_capacity=2000, send_interval=0.0005
    )
    fwd = Forwarder(settings)
    await fwd.start()
    try:
        vni, mac = 300, "00:00:00:00:00:77"
        await fwd.bind(vni, mac, "A")
        total = 400

        async def producer():
            for _ in range(total):
                await fwd.inject(vni, mac, "02:00:00:00:00:01", 1, None, "live")

        prod = asyncio.create_task(producer())
        await asyncio.sleep(0.05)  # A is already draining
        await fwd.bind(vni, mac, "B")
        await prod

        assert await _wait_for(
            lambda: fwd.receivers["A"].total + fwd.receivers["B"].total == total,
            timeout=15.0,
        )
        frames_a = list(fwd.receivers["A"].frames)
        frames_b = list(fwd.receivers["B"].frames)
        assert frames_a, "expected some frames to leave via A before the rebind"
        assert frames_b, "expected the backlog to move to B"

        seqs = sorted(f["seq"] for f in frames_a + frames_b)
        assert seqs == list(range(1, total + 1))  # exactly once, none lost

        seqs_a = [f["seq"] for f in frames_a]
        seqs_b = [f["seq"] for f in frames_b]
        assert seqs_a == sorted(seqs_a)  # per-egress enqueue order preserved
        assert seqs_b == sorted(seqs_b)
    finally:
        await fwd.stop()
