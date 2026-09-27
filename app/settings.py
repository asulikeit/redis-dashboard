"""설정 모델. pxa-common 의 load_section 으로 config.yaml 의 섹션을 읽는다.

    server  : 대시보드 HTTP 서버
    monitor : 모니터링 대상 Redis Cluster
"""
from __future__ import annotations

from typing import Literal, Optional

from pxa_common import load_section
from pxa_common.fastapi import BaseModel, Field, field_validator


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080


class NodeTarget(BaseModel):
    host: str
    port: int = Field(6379, ge=1, le=65535)

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


class MonitorConfig(BaseModel):
    cluster_name: str = "redis-cluster"
    nodes: list[NodeTarget] = Field(default_factory=list)

    username: Optional[str] = None
    password: Optional[str] = None

    executor: Literal["redis-py", "redis-cli"] = "redis-py"
    redis_cli_path: str = "redis-cli"

    collect_interval_seconds: int = Field(10, ge=1, le=60)
    command_timeout_seconds: float = Field(2.0, gt=0, le=30)
    window_minutes: int = Field(10, ge=1, le=10)
    default_refresh: str = "10s"

    memory_basis_when_unlimited: Literal["system", "none"] = "system"
    memory_warn_pct: float = Field(80, gt=0, le=100)
    memory_crit_pct: float = Field(90, gt=0, le=100)

    @field_validator("nodes")
    @classmethod
    def _nodes_required_and_unique(cls, nodes: list[NodeTarget]) -> list[NodeTarget]:
        if not nodes:
            raise ValueError("monitor.nodes 에 모니터링할 Redis 노드를 1개 이상 등록해야 합니다.")
        seen: set[str] = set()
        for n in nodes:
            if n.addr in seen:
                raise ValueError(f"monitor.nodes 에 중복된 노드가 있습니다: {n.addr}")
            seen.add(n.addr)
        return nodes

    @field_validator("username", "password", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        # YAML 에서 빈 값/빈 문자열은 '미사용' 으로 본다
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        return v

    @property
    def window_seconds(self) -> int:
        return self.window_minutes * 60


def get_server_config() -> ServerConfig:
    return load_section("server", ServerConfig)


def get_monitor_config() -> MonitorConfig:
    return load_section("monitor", MonitorConfig)
