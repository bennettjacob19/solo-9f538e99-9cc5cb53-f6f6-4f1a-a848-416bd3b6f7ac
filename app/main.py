"""FastAPI surface for the dual-egress VXLAN drill. Swagger UI at /docs."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .config import Settings
from .forwarder import (
    EGRESS_NAMES,
    Forwarder,
    QueueFullError,
    RebindCapacityError,
    UnknownBindingError,
)
from .vxlan import VNI_MAX, MacError, normalize_mac

DESCRIPTION = """\
Dual-egress VXLAN drill: a VM moved to a new host, and frames still queued at
the old egress must follow it onto the new tunnel.

* Bind or rebind an egress per **(VNI, destination MAC)**.
* Inject numbered Ethernet test frames; each egress encapsulates them in
  VXLAN and sends them to its own local UDP receiver.
* Pause/resume either egress independently; the other keeps sending.
* Each egress has a startup-configured pending-frame limit; a full egress
  refuses new frames.
* A frame counts as sent only after its socket write succeeds. Writes that
  fail with EAGAIN leave the frame queued on its egress (still occupying
  capacity, still migrating with its destination on a rebind) and are
  retried with a paced backoff.
* `POST /api/egresses/{A|B}/block_writes` simulates that egress's socket
  answering EAGAIN; `unblock_writes` releases it — a backed-up, unpaused
  egress resumes sending within 100 ms. Egress state exposes per-egress
  write-attempt counters.
* Rebinding moves the destination's not-yet-sent frames to the new egress
  atomically (they and later frames are each sent once, in enqueue order).
  If the new egress cannot hold them all, the rebind is refused (409) and
  the binding plus both queues stay unchanged; the old egress never leaks
  moved frames, even after its writes recover.
"""


class BindRequest(BaseModel):
    vni: int = Field(..., ge=0, le=VNI_MAX, examples=[100])
    dst_mac: str = Field(..., examples=["00:00:00:00:00:11"])
    egress: str = Field(..., pattern="^[ABab]$", description="target egress: A or B")

    @field_validator("dst_mac")
    @classmethod
    def _mac(cls, v: str) -> str:
        try:
            return normalize_mac(v)
        except MacError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("egress")
    @classmethod
    def _egress(cls, v: str) -> str:
        return v.upper()


class InjectRequest(BaseModel):
    vni: int = Field(..., ge=0, le=VNI_MAX, examples=[100])
    dst_mac: str = Field(..., examples=["00:00:00:00:00:11"])
    src_mac: str = Field("02:00:00:00:00:01", examples=["02:00:00:00:00:01"])
    count: int = Field(1, ge=1, le=100000)
    seq_start: Optional[int] = Field(
        None, ge=0, description="first sequence number; default continues the per-destination counter"
    )
    payload: str = Field("vxlan-drill", max_length=1400)

    @field_validator("dst_mac", "src_mac")
    @classmethod
    def _mac(cls, v: str) -> str:
        try:
            return normalize_mac(v)
        except MacError as exc:
            raise ValueError(str(exc)) from exc


def _egress_name(name: str) -> str:
    n = name.upper()
    if n not in EGRESS_NAMES:
        raise HTTPException(status_code=404, detail=f"unknown egress {name!r}; use A or B")
    return n


def _receiver_name(name: str) -> str:
    n = name.upper()
    if n not in EGRESS_NAMES:
        raise HTTPException(status_code=404, detail=f"unknown receiver {name!r}; use A or B")
    return n


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    forwarder = Forwarder(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await forwarder.start()
        yield
        await forwarder.stop()

    app = FastAPI(title="Dual-egress VXLAN drill", version="1.0.0", description=DESCRIPTION, lifespan=lifespan)
    app.state.forwarder = forwarder
    app.state.settings = settings

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse("/docs")

    @app.get("/api/health", tags=["misc"])
    async def health():
        return {"status": "ok"}

    @app.get("/api/overview", tags=["misc"], summary="Egresses, receivers and bindings in one view")
    async def overview():
        return forwarder.snapshot()

    # -- egress control ----------------------------------------------------
    @app.get("/api/egresses", tags=["egress"], summary="State of both egresses")
    async def list_egresses():
        return {"egresses": [e.snapshot() for e in forwarder.egresses.values()]}

    @app.post("/api/egresses/{name}/pause", tags=["egress"], summary="Pause one egress (the other keeps sending)")
    async def pause_egress(name: str):
        return forwarder.pause(_egress_name(name))

    @app.post("/api/egresses/{name}/resume", tags=["egress"], summary="Resume one egress")
    async def resume_egress(name: str):
        return forwarder.resume(_egress_name(name))

    @app.post(
        "/api/egresses/{name}/block_writes",
        tags=["egress"],
        summary="Simulate this egress's socket answering every write with EAGAIN",
    )
    async def block_writes(name: str):
        return forwarder.block_writes(_egress_name(name))

    @app.post(
        "/api/egresses/{name}/unblock_writes",
        tags=["egress"],
        summary="Release the simulated EAGAIN (a backed-up egress resumes within 100 ms)",
    )
    async def unblock_writes(name: str):
        return forwarder.unblock_writes(_egress_name(name))

    # -- bindings ------------------------------------------------------------
    @app.get("/api/bindings", tags=["bindings"], summary="List (VNI, dst MAC) -> egress bindings")
    async def list_bindings():
        return {"bindings": forwarder.bindings_snapshot()}

    @app.post(
        "/api/bindings",
        tags=["bindings"],
        summary="Bind or rebind (VNI, dst MAC) to an egress",
        responses={409: {"description": "target egress cannot hold the destination's pending frames"}},
    )
    async def bind(req: BindRequest):
        try:
            return await forwarder.bind(req.vni, req.dst_mac, req.egress)
        except RebindCapacityError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    # -- frame injection -------------------------------------------------------
    @app.post(
        "/api/frames",
        tags=["frames"],
        summary="Inject numbered Ethernet test frames",
        responses={409: {"description": "no binding, or bound egress is full"}},
    )
    async def inject(req: InjectRequest):
        try:
            return await forwarder.inject(
                req.vni, req.dst_mac, req.src_mac, req.count, req.seq_start, req.payload
            )
        except (QueueFullError, UnknownBindingError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    # -- receivers ---------------------------------------------------------------
    @app.get("/api/receivers", tags=["receivers"], summary="Summary of both local UDP receivers")
    async def list_receivers():
        return {
            "receivers": [
                {k: v for k, v in r.snapshot(0).items() if k != "frames"}
                for r in forwarder.receivers.values()
            ]
        }

    @app.get(
        "/api/receivers/{name}/frames",
        tags=["receivers"],
        summary="VXLAN frames actually received by one local UDP receiver",
    )
    async def receiver_frames(name: str, limit: int = Query(200, ge=1, le=5000)):
        return forwarder.receivers[_receiver_name(name)].snapshot(limit)

    @app.post("/api/receivers/reset", tags=["receivers"], summary="Clear both receivers' buffers and counters")
    async def reset_receivers():
        for receiver in forwarder.receivers.values():
            receiver.reset()
        return {"reset": True}

    return app


app = create_app()
