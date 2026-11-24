"""Dual-egress VXLAN forwarding core.

Design notes
------------
* Each egress keeps *per-destination* queues keyed by (vni, dst_mac), plus a
  round-robin list of the destinations that currently hold frames.
* Rebinding (vni, dst_mac) to the other egress moves that destination's deque
  object wholesale: O(1) in the number of frames, and other destinations'
  backlogs are never scanned. The stale key left in the source egress's
  round-robin list is dropped lazily by the sender.
* A single asyncio.Lock serializes enqueue / dequeue / rebind, so a rebind is
  atomic with respect to concurrent injection and sending: every frame of the
  destination is sent exactly once, in enqueue order, no matter how the
  operations interleave.
* A frame is dequeued only *after* its socket write succeeds: the sender
  peeks the round-robin head frame, writes it to the UDP socket, and pops it
  only on success — all under the same lock hold, so a rebind can never
  straddle a half-sent frame. If the write fails with EAGAIN (real or
  simulated via the API), the frame stays queued: it keeps occupying the
  egress's capacity, is counted as *not* sent, migrates with its destination
  on a rebind, and is retried after a paced backoff. The old egress therefore
  holds nothing it could leak once the destination has been rebound away.
* The capacity check happens before anything is moved; if the target egress
  cannot hold all of the destination's pending frames, the rebind is refused
  and the binding plus both queues are left untouched.
"""
from __future__ import annotations

import asyncio
import contextlib
import errno
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from .config import Settings
from .vxlan import (
    TEST_ETHERTYPE,
    build_ethernet_frame,
    build_test_payload,
    build_vxlan_packet,
    normalize_mac,
    parse_ethernet_frame,
    parse_test_payload,
    parse_vxlan_packet,
)

Key = Tuple[int, str]  # (vni, dst_mac)

EGRESS_NAMES = ("A", "B")


class QueueFullError(Exception):
    """Egress cannot accept the batch (startup-configured capacity reached)."""


class RebindCapacityError(Exception):
    """Target egress cannot hold all pending frames of the destination."""


class UnknownBindingError(Exception):
    """No binding exists for (vni, dst_mac)."""


@dataclass(frozen=True)
class Frame:
    vni: int
    dst_mac: str
    src_mac: str
    seq: int
    inner: bytes  # complete inner Ethernet frame
    enqueued_at: float


class Egress:
    """One tunnel egress: per-destination queues plus a paced UDP sender."""

    def __init__(
        self, name: str, capacity: int, send_interval: float, retry_interval: float
    ) -> None:
        self.name = name
        self.capacity = capacity
        self.send_interval = send_interval
        self.retry_interval = retry_interval
        self.pending: Dict[Key, Deque[Frame]] = {}
        self.active: Deque[Key] = deque()  # destinations to visit, round-robin
        self.count = 0
        self.paused = False
        self.sent = 0
        # Write accounting: every socket-write attempt is counted, successful
        # or not; write_eagain counts the attempts refused with EAGAIN.
        self.write_attempts = 0
        self.write_eagain = 0
        # Fault injection: simulate this egress's socket answering EAGAIN.
        self.writes_blocked = False
        self.target: Optional[Tuple[str, int]] = None
        # The following are created in start(), inside the running event loop.
        self._lock: Optional[asyncio.Lock] = None
        self._nonempty: Optional[asyncio.Event] = None
        self._resumed: Optional[asyncio.Event] = None
        self._retry_wake: Optional[asyncio.Event] = None
        self._task: "Optional[asyncio.Task[None]]" = None
        self._transport: Optional[asyncio.DatagramTransport] = None

    async def start(self, lock: asyncio.Lock) -> None:
        self._lock = lock
        self._nonempty = asyncio.Event()
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._retry_wake = asyncio.Event()
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=self.target
        )
        self._task = asyncio.create_task(self._sender_loop(), name=f"egress-{self.name}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    # -- mutations, always called under the shared lock -------------------
    def enqueue_locked(self, frame: Frame) -> None:
        key = (frame.vni, frame.dst_mac)
        q = self.pending.get(key)
        if q is None:
            q = deque()
            self.pending[key] = q
            self.active.append(key)
        q.append(frame)
        self.count += 1
        assert self._nonempty is not None
        self._nonempty.set()

    def peek_locked(self) -> Optional[Frame]:
        """Return the round-robin head frame without removing it."""
        assert self._nonempty is not None
        while self.active:
            key = self.active[0]
            q = self.pending.get(key)
            if not q:
                # Stale entry (destination rebound away): drop lazily, O(1).
                self.active.popleft()
                continue
            return q[0]
        if self.count == 0:
            self._nonempty.clear()
        return None

    def pop_head_locked(self) -> Frame:
        """Remove the frame previously returned by peek_locked().

        Only called after the peeked frame was successfully written to the
        socket, with no await in between, so the head cannot have changed.
        """
        assert self._nonempty is not None
        key = self.active[0]
        q = self.pending[key]
        frame = q.popleft()
        self.count -= 1
        if q:
            self.active.rotate(-1)
        else:
            del self.pending[key]
            self.active.popleft()
        if self.count == 0:
            self._nonempty.clear()
        return frame

    # -- pause / resume -----------------------------------------------------
    def pause(self) -> None:
        self.paused = True
        assert self._resumed is not None
        self._resumed.clear()

    def resume(self) -> None:
        self.paused = False
        assert self._resumed is not None
        self._resumed.set()
        assert self._retry_wake is not None
        self._retry_wake.set()  # cut short a retry backoff

    # -- fault injection ------------------------------------------------------
    def block_writes(self) -> None:
        """Simulate this egress's UDP socket answering every write with EAGAIN."""
        self.writes_blocked = True

    def unblock_writes(self) -> None:
        """Release the simulated EAGAIN and wake a backed-up sender at once."""
        self.writes_blocked = False
        assert self._retry_wake is not None
        self._retry_wake.set()

    # -- sender ---------------------------------------------------------------
    def _write(self, packet: bytes) -> None:
        """One socket-write attempt; raises BlockingIOError on EAGAIN."""
        if self.writes_blocked:
            raise BlockingIOError(errno.EAGAIN, "simulated EAGAIN (writes blocked via API)")
        assert self._transport is not None
        self._transport.sendto(packet)

    async def _try_send_head(self) -> bool:
        """Attempt to send the head frame once. Return True if it hit EAGAIN."""
        assert self._lock is not None
        async with self._lock:
            if self.paused:
                return False
            frame = self.peek_locked()
            if frame is None:
                return False
            packet = build_vxlan_packet(frame.vni, frame.inner)
            self.write_attempts += 1
            try:
                self._write(packet)
            except BlockingIOError:
                # Not sent: the frame stays queued, keeps occupying this
                # egress's capacity, and still migrates with its destination
                # on a rebind. The lock is held across peek/write, so no
                # rebind can straddle this attempt.
                self.write_eagain += 1
                return True
            # The socket accepted the frame: only now dequeue and count it.
            self.pop_head_locked()
            self.sent += 1
            return False

    async def _sender_loop(self) -> None:
        assert self._resumed is not None and self._nonempty is not None
        assert self._retry_wake is not None
        while True:
            await self._resumed.wait()
            await self._nonempty.wait()
            eagain = await self._try_send_head()
            if eagain:
                # Paced retry: with the default 0.06 s interval a
                # continuously unwritable egress attempts < 20 writes/s, and
                # a released egress (event set by unblock/resume) retries
                # immediately — well within 100 ms.
                self._retry_wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._retry_wake.wait(), self.retry_interval)
            elif self.send_interval:
                await asyncio.sleep(self.send_interval)

    # -- introspection --------------------------------------------------------
    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "capacity": self.capacity,
            "pending": self.count,
            "paused": self.paused,
            "sent": self.sent,
            "writes_blocked": self.writes_blocked,
            "write_attempts": self.write_attempts,
            "write_eagain": self.write_eagain,
            "target": f"{self.target[0]}:{self.target[1]}" if self.target else None,
            "destinations": [
                {"vni": key[0], "dst_mac": key[1], "pending": len(q)}
                for key, q in sorted(self.pending.items())
            ],
        }


class _ReceiverProtocol(asyncio.DatagramProtocol):
    def __init__(self, receiver: "Receiver") -> None:
        self.receiver = receiver

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.receiver.on_datagram(data, addr)


class Receiver:
    """Local UDP endpoint that decapsulates VXLAN frames and records them."""

    def __init__(self, name: str, host: str, port: int, buffer_size: int) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.frames: Deque[dict] = deque(maxlen=buffer_size)
        self.total = 0
        self.errors = 0
        self._transport: Optional[asyncio.DatagramTransport] = None

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _ReceiverProtocol(self), local_addr=(self.host, self.port)
        )
        # Resolve the real port (relevant when configured port is 0).
        self.port = self._transport.get_extra_info("sockname")[1]

    async def stop(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    def on_datagram(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.total += 1
        record: dict = {
            "index": self.total,
            "receiver": self.name,
            "from": f"{addr[0]}:{addr[1]}",
            "received_at": time.time(),
        }
        try:
            vni, inner = parse_vxlan_packet(data)
            dst, src, ethertype, payload = parse_ethernet_frame(inner)
            seq, text = parse_test_payload(payload)
            record.update(
                vni=vni,
                dst_mac=dst,
                src_mac=src,
                ethertype=f"0x{ethertype:04x}",
                seq=seq,
                payload=text,
            )
        except ValueError as exc:
            self.errors += 1
            record.update(error=str(exc), raw=data.hex())
        self.frames.append(record)

    def reset(self) -> None:
        self.frames.clear()
        self.total = 0
        self.errors = 0

    def snapshot(self, limit: int = 100) -> dict:
        frames = list(self.frames)[-limit:]
        return {
            "name": self.name,
            "listen": f"{self.host}:{self.port}",
            "total_received": self.total,
            "decode_errors": self.errors,
            "buffered": len(self.frames),
            "frames": frames,
        }


class Forwarder:
    """Binding table plus two egresses plus two local VXLAN receivers."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.egresses: Dict[str, Egress] = {
            "A": Egress(
                "A", settings.egress_a_capacity, settings.send_interval, settings.write_retry_interval
            ),
            "B": Egress(
                "B", settings.egress_b_capacity, settings.send_interval, settings.write_retry_interval
            ),
        }
        self.receivers: Dict[str, Receiver] = {
            "A": Receiver("A", settings.receiver_host, settings.receiver_a_port, settings.received_buffer),
            "B": Receiver("B", settings.receiver_host, settings.receiver_b_port, settings.received_buffer),
        }
        self.bindings: Dict[Key, str] = {}
        self._next_seq: Dict[Key, int] = {}
        self._lock: Optional[asyncio.Lock] = None

    async def start(self) -> None:
        self._lock = asyncio.Lock()
        for receiver in self.receivers.values():
            await receiver.start()
        # Local drill: egress A tunnels to receiver A, egress B to receiver B.
        for name, egress in self.egresses.items():
            egress.target = (self.settings.send_host, self.receivers[name].port)
        for egress in self.egresses.values():
            await egress.start(self._lock)

    async def stop(self) -> None:
        for egress in self.egresses.values():
            await egress.stop()
        for receiver in self.receivers.values():
            await receiver.stop()

    # -- binding / rebinding -------------------------------------------------
    async def bind(self, vni: int, dst_mac: str, egress_name: str) -> dict:
        """Bind (vni, dst_mac) to an egress, or rebind it, moving its backlog.

        The move is atomic with respect to concurrent injection and sending.
        If the target egress cannot hold every pending frame of the
        destination, RebindCapacityError is raised and nothing changes.
        """
        assert self._lock is not None
        mac = normalize_mac(dst_mac)
        key = (vni, mac)
        async with self._lock:
            target = self.egresses[egress_name]
            current = self.bindings.get(key)
            if current == egress_name:
                return {
                    "vni": vni,
                    "dst_mac": mac,
                    "egress": egress_name,
                    "previous_egress": current,
                    "changed": False,
                    "moved_pending": 0,
                    "target_pending": target.count,
                    "target_capacity": target.capacity,
                }
            moved = 0
            if current is not None:
                source = self.egresses[current]
                q = source.pending.get(key)
                n = len(q) if q is not None else 0
                if target.count + n > target.capacity:
                    raise RebindCapacityError(
                        f"egress {egress_name} cannot hold {n} pending frame(s) of "
                        f"(vni={vni}, mac={mac}): free capacity "
                        f"{target.capacity - target.count}; "
                        f"binding and both queues left unchanged"
                    )
                if n:
                    # O(1) move of this destination's deque; other
                    # destinations' backlogs are never scanned. The stale key
                    # in source.active is dropped lazily by the sender.
                    del source.pending[key]
                    source.count -= n
                    if source.count == 0 and source._nonempty is not None:
                        source._nonempty.clear()
                    existing = target.pending.get(key)
                    if existing is None:
                        target.pending[key] = q
                        target.active.append(key)
                    else:
                        existing.extend(q)
                    target.count += n
                    if target._nonempty is not None:
                        target._nonempty.set()
                    moved = n
            self.bindings[key] = egress_name
            return {
                "vni": vni,
                "dst_mac": mac,
                "egress": egress_name,
                "previous_egress": current,
                "changed": True,
                "moved_pending": moved,
                "target_pending": target.count,
                "target_capacity": target.capacity,
            }

    # -- injection -------------------------------------------------------------
    async def inject(
        self,
        vni: int,
        dst_mac: str,
        src_mac: str,
        count: int,
        seq_start: Optional[int],
        text: str,
    ) -> dict:
        """Enqueue `count` numbered test frames for (vni, dst_mac).

        The batch is atomic: if the bound egress cannot hold every frame,
        nothing is enqueued and QueueFullError is raised.
        """
        assert self._lock is not None
        mac = normalize_mac(dst_mac)
        src = normalize_mac(src_mac)
        key = (vni, mac)
        async with self._lock:
            egress_name = self.bindings.get(key)
            if egress_name is None:
                raise UnknownBindingError(
                    f"no binding for (vni={vni}, mac={mac}); "
                    f"create one via POST /api/bindings"
                )
            egress = self.egresses[egress_name]
            if egress.count + count > egress.capacity:
                raise QueueFullError(
                    f"egress {egress_name} full: pending {egress.count}/"
                    f"{egress.capacity}, refused {count} frame(s)"
                )
            start = seq_start if seq_start is not None else self._next_seq.get(key, 1)
            for i in range(count):
                seq = start + i
                inner = build_ethernet_frame(
                    mac, src, TEST_ETHERTYPE, build_test_payload(seq, text)
                )
                egress.enqueue_locked(Frame(vni, mac, src, seq, inner, time.time()))
            self._next_seq[key] = start + count
            return {
                "vni": vni,
                "dst_mac": mac,
                "egress": egress_name,
                "enqueued": count,
                "seq_start": start,
                "seq_end": start + count - 1,
                "egress_pending": egress.count,
                "egress_capacity": egress.capacity,
            }

    # -- pause / resume ----------------------------------------------------------
    def pause(self, name: str) -> dict:
        self.egresses[name].pause()
        return self.egresses[name].snapshot()

    def resume(self, name: str) -> dict:
        self.egresses[name].resume()
        return self.egresses[name].snapshot()

    # -- fault injection -----------------------------------------------------------
    def block_writes(self, name: str) -> dict:
        self.egresses[name].block_writes()
        return self.egresses[name].snapshot()

    def unblock_writes(self, name: str) -> dict:
        self.egresses[name].unblock_writes()
        return self.egresses[name].snapshot()

    # -- introspection -------------------------------------------------------------
    def bindings_snapshot(self) -> List[dict]:
        return [
            {"vni": key[0], "dst_mac": key[1], "egress": egress_name}
            for key, egress_name in sorted(self.bindings.items())
        ]

    def snapshot(self) -> dict:
        return {
            "egresses": [e.snapshot() for e in self.egresses.values()],
            "receivers": [
                {k: v for k, v in r.snapshot(0).items() if k != "frames"}
                for r in self.receivers.values()
            ],
            "bindings": self.bindings_snapshot(),
        }
