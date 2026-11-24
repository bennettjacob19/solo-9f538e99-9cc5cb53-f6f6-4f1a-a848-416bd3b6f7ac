"""Minimal VXLAN (RFC 7348) and Ethernet helpers for the drill."""
from __future__ import annotations

VXLAN_FLAGS = 0x08  # I flag: valid VNI
VXLAN_HEADER_LEN = 8
VNI_MAX = 0xFFFFFF
TEST_ETHERTYPE = 0x88B5  # IEEE 802.1 local experimental ethertype
TEST_MAGIC = b"VXDR"


class MacError(ValueError):
    pass


def normalize_mac(mac: str) -> str:
    """Return canonical lowercase colon-separated MAC, or raise MacError."""
    text = mac.strip().lower().replace("-", ":")
    parts = text.split(":")
    if len(parts) == 1 and len(parts[0]) == 12:  # aabbccddeeff form
        parts = [parts[0][i : i + 2] for i in range(0, 12, 2)]
    if len(parts) != 6 or any(len(p) != 2 for p in parts):
        raise MacError(f"invalid MAC address: {mac!r}")
    try:
        bytes(int(p, 16) for p in parts)
    except ValueError as exc:
        raise MacError(f"invalid MAC address: {mac!r}") from exc
    return ":".join(parts)


def mac_to_bytes(mac: str) -> bytes:
    return bytes(int(p, 16) for p in normalize_mac(mac).split(":"))


def bytes_to_mac(raw: bytes) -> str:
    return ":".join(f"{b:02x}" for b in raw)


def build_vxlan_packet(vni: int, inner_frame: bytes) -> bytes:
    if not 0 <= vni <= VNI_MAX:
        raise ValueError(f"VNI out of range: {vni}")
    header = bytes([VXLAN_FLAGS, 0, 0, 0]) + vni.to_bytes(3, "big") + b"\x00"
    return header + inner_frame


def parse_vxlan_packet(data: bytes) -> tuple[int, bytes]:
    """Return (vni, inner_frame)."""
    if len(data) < VXLAN_HEADER_LEN:
        raise ValueError("packet shorter than VXLAN header")
    if data[0] != VXLAN_FLAGS:
        raise ValueError(f"unexpected VXLAN flags: {data[0]:#04x}")
    vni = int.from_bytes(data[4:7], "big")
    return vni, data[VXLAN_HEADER_LEN:]


def build_ethernet_frame(dst_mac: str, src_mac: str, ethertype: int, payload: bytes) -> bytes:
    return mac_to_bytes(dst_mac) + mac_to_bytes(src_mac) + ethertype.to_bytes(2, "big") + payload


def parse_ethernet_frame(data: bytes) -> tuple[str, str, int, bytes]:
    """Return (dst_mac, src_mac, ethertype, payload)."""
    if len(data) < 14:
        raise ValueError("frame shorter than Ethernet header")
    dst = bytes_to_mac(data[0:6])
    src = bytes_to_mac(data[6:12])
    ethertype = int.from_bytes(data[12:14], "big")
    return dst, src, ethertype, data[14:]


def build_test_payload(seq: int, text: str) -> bytes:
    return TEST_MAGIC + seq.to_bytes(4, "big") + text.encode("utf-8", "replace")


def parse_test_payload(payload: bytes) -> tuple[int | None, str]:
    """Return (seq, text) for drill frames; (None, hex) otherwise."""
    if payload.startswith(TEST_MAGIC) and len(payload) >= 8:
        seq = int.from_bytes(payload[4:8], "big")
        return seq, payload[8:].decode("utf-8", "replace")
    return None, payload.hex()
