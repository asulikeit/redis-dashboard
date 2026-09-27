"""redis-cli 원문 출력 파서 (INFO / CLUSTER INFO / CLUSTER NODES)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# CLUSTER NODES 에서 이상 상태로 보는 플래그 (요건 2)
PROBLEM_FLAGS = ("fail", "fail?", "handshake", "noaddr")


def parse_kv(text: str) -> dict[str, str]:
    """`key:value` 줄 목록을 dict 로. INFO, CLUSTER INFO 공통."""
    result: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        result[key.strip()] = value.strip()
    return result


def to_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError:
            return None


@dataclass
class ClusterNodeEntry:
    node_id: str
    addr: str                      # host:port (cluster bus port 제외)
    flags: set[str]
    master_id: Optional[str]
    link_state: str
    slots: list[str] = field(default_factory=list)

    @property
    def is_master(self) -> bool:
        return "master" in self.flags

    @property
    def is_myself(self) -> bool:
        return "myself" in self.flags

    @property
    def problem_flags(self) -> list[str]:
        return [f for f in PROBLEM_FLAGS if f in self.flags]


def _normalize_addr(raw: str) -> str:
    """`ip:port@cport[,hostname]` -> `ip:port`. IPv6 는 마지막 ':' 기준으로 자른다."""
    addr = raw.split(",", 1)[0].split("@", 1)[0]
    host, _, port = addr.rpartition(":")
    return f"{host.strip('[]')}:{port}" if host else addr


def parse_cluster_nodes(text: str) -> list[ClusterNodeEntry]:
    """CLUSTER NODES 출력.

    <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <epoch> <link-state> <slot>...
    """
    entries: list[ClusterNodeEntry] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 8:
            continue
        entries.append(
            ClusterNodeEntry(
                node_id=parts[0],
                addr=_normalize_addr(parts[1]),
                flags=set(parts[2].split(",")),
                master_id=None if parts[3] == "-" else parts[3],
                link_state=parts[7],
                slots=parts[8:],
            )
        )
    return entries
