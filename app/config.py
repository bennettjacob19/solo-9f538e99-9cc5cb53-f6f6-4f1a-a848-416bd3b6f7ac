"""Startup configuration, populated from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else int(raw)


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else float(raw)


@dataclass
class Settings:
    # The two local UDP receivers that terminate the VXLAN tunnels.
    receiver_host: str = "0.0.0.0"
    receiver_a_port: int = 4789
    receiver_b_port: int = 4790
    # Where egresses send VXLAN packets (local drill -> loopback).
    send_host: str = "127.0.0.1"
    # Max pending frames per egress, set at startup. Full -> enqueue refused.
    egress_a_capacity: int = 100
    egress_b_capacity: int = 100
    # Pacing between frames per egress, in seconds (0 = as fast as possible).
    send_interval: float = 0.0
    # How many received frames each receiver keeps for inspection.
    received_buffer: int = 5000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            receiver_host=os.environ.get("RECEIVER_HOST", "0.0.0.0"),
            receiver_a_port=_int("RECEIVER_A_PORT", 4789),
            receiver_b_port=_int("RECEIVER_B_PORT", 4790),
            send_host=os.environ.get("SEND_HOST", "127.0.0.1"),
            egress_a_capacity=_int("EGRESS_A_CAPACITY", 100),
            egress_b_capacity=_int("EGRESS_B_CAPACITY", 100),
            send_interval=_float("SEND_INTERVAL", 0.0),
            received_buffer=_int("RECEIVED_BUFFER", 5000),
        )
