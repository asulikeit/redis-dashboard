"""주기 수집기.

매 주기마다 등록된 모든 노드에 명령을 병렬로 보내고, 결과를
  - 시계열(MetricStore) 에 한 샘플
  - 화면용 스냅샷(dict) 한 벌
로 만든다. 화면 요청은 마지막 스냅샷을 그대로 읽기만 하므로
브라우저 수/refresh 주기와 무관하게 Redis 에 가는 부하는 일정하다.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .executor import CommandError, Executor
from .parsers import ClusterNodeEntry, parse_cluster_nodes, parse_kv, to_int
from .settings import MonitorConfig, NodeTarget
from .store import MetricStore, NodeSample, Sample

logger = logging.getLogger("pxa.redis_dashboard.collector")

TOTAL_SLOTS = 16384
_SEVERITY = {"ok": 0, "unknown": 1, "warn": 2, "fail": 3}


@dataclass
class NodeResult:
    index: int
    target: NodeTarget
    up: bool = False
    ping: Optional[str] = None
    error: Optional[str] = None
    command_errors: dict[str, str] = field(default_factory=dict)

    node_id: Optional[str] = None
    role: Optional[str] = None             # master | replica
    master_addr: Optional[str] = None      # replica 인 경우 따라가는 master
    cluster_info: Optional[dict[str, str]] = None
    cluster_nodes: Optional[list[ClusterNodeEntry]] = None

    dbsize: Optional[int] = None
    used_memory: Optional[int] = None
    maxmemory: Optional[int] = None
    total_system_memory: Optional[int] = None
    mem_pct: Optional[float] = None
    mem_basis: Optional[str] = None        # maxmemory | system | unlimited
    evicted_total: Optional[int] = None
    ops: Optional[int] = None
    clients: Optional[int] = None

    @property
    def addr(self) -> str:
        return self.target.addr


class Collector:
    def __init__(self, cfg: MonitorConfig, executor: Executor, store: MetricStore):
        self.cfg = cfg
        self.executor = executor
        self.store = store
        self.snapshot: Optional[dict] = None
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="redis-collector")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.executor.close()

    async def _loop(self) -> None:
        interval = self.cfg.collect_interval_seconds
        while True:
            started = time.monotonic()
            try:
                await self.collect_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("수집 중 예기치 않은 오류")
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))

    # ------------------------------------------------------------------ 수집
    async def collect_once(self) -> dict:
        t0 = time.monotonic()
        results = await asyncio.gather(
            *(self._collect_node(i, t) for i, t in enumerate(self.cfg.nodes))
        )
        now = time.time()
        self.store.add(
            Sample(
                ts=now,
                nodes={
                    r.addr: NodeSample(r.mem_pct, r.evicted_total, r.ops, r.clients)
                    for r in results
                    if r.up
                },
            )
        )
        self.snapshot = self._build_snapshot(results, now, (time.monotonic() - t0) * 1000)
        down = [r.addr for r in results if not r.up]
        if down:
            logger.warning("응답 없는 노드: %s", ", ".join(down))
        return self.snapshot

    async def _collect_node(self, index: int, t: NodeTarget) -> NodeResult:
        r = NodeResult(index=index, target=t)
        run = self.executor.run

        # 8. PING -> PONG
        try:
            r.ping = (await run(t, "PING")).strip()
        except CommandError as e:
            r.error = str(e)
            return r
        r.up = r.ping.upper() == "PONG"
        if not r.up:
            r.error = f"PING 응답이 PONG 이 아닙니다: {r.ping!r}"
            return r

        commands = {
            "INFO memory": ("INFO", "memory"),              # 6
            "INFO stats": ("INFO", "stats"),                # 7, OPS/sec
            "INFO replication": ("INFO", "replication"),    # 8
            "INFO clients": ("INFO", "clients"),            # Clients
            "CLUSTER INFO": ("CLUSTER", "INFO"),            # 1, 3, 4
            "CLUSTER NODES": ("CLUSTER", "NODES"),          # 2
        }
        outputs = await asyncio.gather(
            *(run(t, *args) for args in commands.values()), return_exceptions=True
        )
        raw: dict[str, str] = {}
        for name, out in zip(commands, outputs):
            if isinstance(out, BaseException):
                r.command_errors[name] = str(out) or type(out).__name__
            else:
                raw[name] = out

        if "INFO memory" in raw:
            mem = parse_kv(raw["INFO memory"])
            r.used_memory = to_int(mem.get("used_memory"))
            r.maxmemory = to_int(mem.get("maxmemory"))
            r.total_system_memory = to_int(mem.get("total_system_memory"))
            r.mem_pct, r.mem_basis = self._memory_pct(r)

        if "INFO stats" in raw:
            stats = parse_kv(raw["INFO stats"])
            r.evicted_total = to_int(stats.get("evicted_keys"))
            r.ops = to_int(stats.get("instantaneous_ops_per_sec"))

        if "INFO clients" in raw:
            r.clients = to_int(parse_kv(raw["INFO clients"]).get("connected_clients"))

        if "INFO replication" in raw:
            repl = parse_kv(raw["INFO replication"])
            role = repl.get("role")
            if role == "master":
                r.role = "master"
            elif role in ("slave", "replica"):
                r.role = "replica"
                if repl.get("master_host"):
                    r.master_addr = f"{repl['master_host']}:{repl.get('master_port', '?')}"

        if "CLUSTER INFO" in raw:
            r.cluster_info = parse_kv(raw["CLUSTER INFO"])

        if "CLUSTER NODES" in raw:
            r.cluster_nodes = parse_cluster_nodes(raw["CLUSTER NODES"])
            me = next((e for e in r.cluster_nodes if e.is_myself), None)
            if me is not None:
                r.node_id = me.node_id
                if r.role is None:   # INFO replication 실패 시 보조 판단
                    r.role = "master" if me.is_master else "replica"

        # 5. DBSIZE 는 master 에서만
        if r.role == "master":
            try:
                r.dbsize = to_int(await run(t, "DBSIZE"))
            except CommandError as e:
                r.command_errors["DBSIZE"] = str(e)

        for name, err in r.command_errors.items():
            logger.warning("%s %s 실패: %s", r.addr, name, err)
        return r

    def _memory_pct(self, r: NodeResult) -> tuple[Optional[float], str]:
        if r.used_memory is None:
            return None, "unknown"
        if r.maxmemory:
            return r.used_memory / r.maxmemory * 100, "maxmemory"
        if self.cfg.memory_basis_when_unlimited == "system" and r.total_system_memory:
            return r.used_memory / r.total_system_memory * 100, "system"
        return None, "unlimited"

    # ------------------------------------------------------------------ 스냅샷
    def _build_snapshot(self, results: list[NodeResult], now: float, took_ms: float) -> dict:
        cfg = self.cfg
        checks = self._cluster_checks(results)

        nodes: list[dict] = []
        mem_crit: list[str] = []
        mem_warn: list[str] = []
        evicting: list[str] = []
        for r in results:
            ev = self.store.evictions_in_window(r.addr) if r.up else None
            mem_status = "unknown"
            if r.mem_pct is not None:
                if r.mem_pct >= cfg.memory_crit_pct:
                    mem_status = "fail"
                elif r.mem_pct >= cfg.memory_warn_pct:
                    mem_status = "warn"
                else:
                    mem_status = "ok"
            if mem_status in ("warn", "fail"):
                basis = ", 시스템 기준" if r.mem_basis == "system" else ""
                (mem_crit if mem_status == "fail" else mem_warn).append(f"{r.addr} {r.mem_pct:.1f}%{basis}")
            if ev and ev.count:
                evicting.append(f"{r.addr} {ev.count:,}건")

            nodes.append({
                "index": r.index,
                "addr": r.addr,
                "up": r.up,
                "ping": r.ping,
                "error": r.error,
                "command_errors": r.command_errors,
                "node_id": r.node_id,
                "role": r.role,
                "master_addr": r.master_addr,
                "cluster_state": (r.cluster_info or {}).get("cluster_state"),
                "dbsize": r.dbsize,
                "used_memory": r.used_memory,
                "maxmemory": r.maxmemory,
                "total_system_memory": r.total_system_memory,
                "mem_pct": None if r.mem_pct is None else round(r.mem_pct, 2),
                "mem_basis": r.mem_basis,
                "mem_status": mem_status,
                "evicted_window": None if ev is None else ev.count,
                "evicted_window_seconds": None if ev is None else round(ev.covered_seconds),
                "evicted_restarted": bool(ev and ev.restarted),
                "evicted_total": r.evicted_total,
                "ops": r.ops,
                "clients": r.clients,
            })

        node_issues: list[tuple[str, str]] = []   # (severity, 문장)
        if mem_crit:
            node_issues.append(("fail", f"메모리 {cfg.memory_crit_pct:g}% 이상: " + ", ".join(mem_crit)))
        if mem_warn:
            node_issues.append(("warn", f"메모리 {cfg.memory_warn_pct:g}% 이상: " + ", ".join(mem_warn)))
        if evicting:
            node_issues.append(("warn", f"최근 {cfg.window_minutes}분 eviction 발생: " + ", ".join(evicting)))

        verdict = self._verdict(checks, node_issues, results)
        return {
            "cluster_name": cfg.cluster_name,
            "collected_at": now,
            "collect_ms": round(took_ms),
            "collect_interval": cfg.collect_interval_seconds,
            "window_seconds": cfg.window_seconds,
            "memory_warn_pct": cfg.memory_warn_pct,
            "memory_crit_pct": cfg.memory_crit_pct,
            "verdict": verdict,
            "checks": checks,
            "nodes": nodes,
            "series": self.store.series([r.addr for r in results]),
        }

    def _cluster_checks(self, results: list[NodeResult]) -> list[dict]:
        up = [r for r in results if r.up]
        reporters = [(r.addr, r.cluster_info) for r in up if r.cluster_info is not None]
        cluster_disabled = any(
            "cluster support disabled" in r.command_errors.get("CLUSTER INFO", "") for r in up
        )
        unknown_detail = (
            "클러스터 모드가 꺼진 인스턴스입니다 (cluster-enabled no)"
            if cluster_disabled
            else "CLUSTER INFO / NODES 에 응답한 노드가 없습니다"
        )
        checks: list[dict] = []

        # 대상 노드 응답 (PING)
        down = [r.addr for r in results if not r.up]
        checks.append(_check(
            "ping", "대상 노드 응답", f"{len(up)} / {len(results)}", f"{len(results)} / {len(results)}",
            "ok" if not down else ("fail" if not up else "warn"),
            "모든 노드가 PONG 응답" if not down else "응답 없음: " + ", ".join(down),
        ))

        # 1. Cluster State
        if not reporters:
            checks.append(_check("state", "Cluster State", "확인 불가", "ok", "unknown", unknown_detail))
        else:
            bad = [f"{a}={i.get('cluster_state', '?')}" for a, i in reporters if i.get("cluster_state") != "ok"]
            checks.append(_check(
                "state", "Cluster State", "ok" if not bad else "fail", "ok",
                "ok" if not bad else "fail",
                f"응답한 {len(reporters)}개 노드 모두 cluster_state:ok" if not bad else "ok 가 아닌 노드: " + ", ".join(bad),
            ))

        # 2. Nodes
        checks.append(self._nodes_check(results, up, unknown_detail))

        # 3. Slots  (노드마다 보는 값이 다를 수 있어 가장 나쁜 값을 쓴다)
        if not reporters:
            checks.append(_check("slots", "Slots (assigned / ok)", "확인 불가", f"{TOTAL_SLOTS} / {TOTAL_SLOTS}", "unknown", unknown_detail))
        else:
            assigned = min(to_int(i.get("cluster_slots_assigned")) or 0 for _, i in reporters)
            ok = min(to_int(i.get("cluster_slots_ok")) or 0 for _, i in reporters)
            good = assigned == TOTAL_SLOTS and ok == TOTAL_SLOTS
            detail = "모든 슬롯이 할당되고 정상" if good else (
                f"할당되지 않은 슬롯 {TOTAL_SLOTS - assigned:,}개" if assigned < TOTAL_SLOTS
                else f"정상이 아닌 슬롯 {assigned - ok:,}개"
            )
            checks.append(_check("slots", "Slots (assigned / ok)", f"{assigned:,} / {ok:,}", f"{TOTAL_SLOTS:,} / {TOTAL_SLOTS:,}", "ok" if good else "fail", detail))

        # 4. Fail / PFail
        if not reporters:
            checks.append(_check("failpfail", "Slots fail / pfail", "확인 불가", "0 / 0", "unknown", unknown_detail))
        else:
            fail = max(to_int(i.get("cluster_slots_fail")) or 0 for _, i in reporters)
            pfail = max(to_int(i.get("cluster_slots_pfail")) or 0 for _, i in reporters)
            status = "fail" if fail else ("warn" if pfail else "ok")
            detail = {
                "ok": "fail / pfail 슬롯 없음",
                "warn": "일부 노드가 도달 불가로 의심(pfail)하는 슬롯이 있습니다",
                "fail": "과반 노드가 장애로 합의한(fail) 슬롯이 있습니다",
            }[status]
            checks.append(_check("failpfail", "Slots fail / pfail", f"{fail:,} / {pfail:,}", "0 / 0", status, detail))

        return checks

    def _nodes_check(self, results: list[NodeResult], up: list[NodeResult], unknown_detail: str) -> dict:
        views = [r for r in up if r.cluster_nodes is not None]
        if not views:
            return _check("nodes", "Nodes", "확인 불가", "이상 플래그 없음", "unknown", unknown_detail)

        # pfail(fail?) 은 관찰한 노드에만 보이므로 모든 노드의 시각을 합친다
        problems: dict[str, dict] = {}
        for r in views:
            for e in r.cluster_nodes or []:
                for flag in e.problem_flags:
                    p = problems.setdefault(e.node_id, {"addr": e.addr, "flags": set(), "seen_by": set()})
                    p["flags"].add(flag)
                    p["seen_by"].add(r.addr)

        ref = views[0].cluster_nodes or []
        masters = sum(1 for e in ref if e.is_master)
        replicas = len(ref) - masters

        configured = {t.addr for t in self.cfg.nodes}
        known_ids = {r.node_id for r in up if r.node_id}
        unregistered = [e.addr for e in ref if e.node_id not in known_ids and e.addr not in configured]

        flags_all = set().union(*(p["flags"] for p in problems.values())) if problems else set()
        if flags_all & {"fail", "noaddr"}:
            status = "fail"
        elif flags_all or unregistered:
            status = "warn"
        else:
            status = "ok"

        parts: list[str] = []
        for p in problems.values():
            parts.append(f"{p['addr']} {'/'.join(sorted(p['flags']))} ({len(p['seen_by'])}개 노드가 관측)")
        if unregistered:
            parts.append("config 에 없는 노드: " + ", ".join(unregistered))
        detail = "; ".join(parts) if parts else "fail, fail?, handshake, noaddr 없음"

        return _check(
            "nodes", "Nodes", f"{len(ref)}개 (master {masters} / replica {replicas})",
            "이상 플래그 없음", status, detail,
        )

    def _verdict(self, checks: list[dict], node_issues: list[tuple[str, str]], results: list[NodeResult]) -> dict:
        # 같은 원인(예: 응답 노드 없음)으로 여러 항목이 실패하면 한 줄로 묶는다
        grouped: dict[tuple[str, str], list[str]] = {}
        for c in checks:
            if c["status"] != "ok":
                grouped.setdefault((c["status"], c["detail"]), []).append(c["label"])
        items: list[tuple[str, str]] = [
            (status, f"{', '.join(labels)}: {detail}") for (status, detail), labels in grouped.items()
        ] + node_issues
        worst = max((s for s, _ in items), key=_SEVERITY.__getitem__, default="ok")
        items.sort(key=lambda x: -_SEVERITY[x[0]])

        titles = {
            "ok": "클러스터 정상",
            "warn": "확인이 필요합니다",
            "fail": "클러스터 이상",
            "unknown": "상태를 확인할 수 없습니다",
        }
        up = sum(1 for r in results if r.up)
        slots = next((c["value"] for c in checks if c["key"] == "slots"), "?")
        return {
            "status": worst,
            "title": titles[worst],
            "summary": f"노드 {up}/{len(results)} 응답, 슬롯 {slots}",
            "issues": [{"status": s, "text": t} for s, t in items],
        }


def _check(key: str, label: str, value: str, expected: str, status: str, detail: str) -> dict:
    return {"key": key, "label": label, "value": value, "expected": expected, "status": status, "detail": detail}

