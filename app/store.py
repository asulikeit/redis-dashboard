"""최근 N분(최대 10분) 시계열을 메모리 링버퍼로 보관한다.

10분 x 10초 주기 = 노드당 60포인트 수준이라 DB(sqlite 등)는 쓰지 않는다.
프로세스 재시작 시 추이는 비어서 다시 쌓인다.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class NodeSample:
    mem_pct: Optional[float]
    evicted_total: Optional[int]      # INFO stats 의 누적 evicted_keys
    ops: Optional[int]                # instantaneous_ops_per_sec
    clients: Optional[int]            # connected_clients


@dataclass(slots=True)
class Sample:
    ts: float
    nodes: dict[str, NodeSample]


@dataclass(slots=True)
class EvictionWindow:
    count: Optional[int]              # 윈도우 동안 evict 건수
    covered_seconds: float            # 실제로 비교한 기간 (기동 직후엔 10분보다 짧다)
    restarted: bool                   # 중간에 카운터가 줄어듦 = 노드 재시작/CONFIG RESETSTAT


class MetricStore:
    def __init__(self, window_seconds: int, interval_seconds: int):
        self.window_seconds = window_seconds
        self.interval_seconds = interval_seconds
        # '10분 전 값' 을 비교하려면 윈도우 바로 바깥 샘플 하나가 더 필요하다
        self._keep_seconds = window_seconds + interval_seconds * 2
        self._samples: deque[Sample] = deque()

    def add(self, sample: Sample) -> None:
        self._samples.append(sample)
        cutoff = sample.ts - self._keep_seconds
        while self._samples and self._samples[0].ts < cutoff:
            self._samples.popleft()

    def __len__(self) -> int:
        return len(self._samples)

    # ------------------------------------------------------------------ 차트용
    def series(self, node_addrs: list[str]) -> dict:
        """최근 윈도우의 차트 데이터. evictions 는 직전 샘플 대비 증가분."""
        if not self._samples:
            return {"ts": [], "nodes": {a: _empty_series() for a in node_addrs}}

        now = self._samples[-1].ts
        start = now - self.window_seconds
        samples = list(self._samples)
        first_idx = next(i for i, s in enumerate(samples) if s.ts >= start)

        ts = [round(s.ts, 3) for s in samples[first_idx:]]
        out: dict[str, dict[str, list]] = {}
        for addr in node_addrs:
            series = _empty_series()
            prev_evicted = _last_known_evicted(samples[:first_idx], addr)
            for s in samples[first_idx:]:
                ns = s.nodes.get(addr)
                series["mem_pct"].append(None if ns is None else _round(ns.mem_pct, 2))
                series["ops"].append(None if ns is None else ns.ops)
                series["clients"].append(None if ns is None else ns.clients)
                cur = None if ns is None else ns.evicted_total
                series["evictions"].append(_delta(prev_evicted, cur))
                if cur is not None:
                    prev_evicted = cur
            out[addr] = series
        return {"ts": ts, "nodes": out}

    # ------------------------------------------------------------------ 요건 7
    def evictions_in_window(self, addr: str) -> EvictionWindow:
        """최근 윈도우(10분) 동안의 evicted_keys 증가 건수.

        기준값은 '윈도우 시작 시각 이전의 마지막 샘플' 이고, 없으면(기동 직후)
        가장 오래된 샘플이다. 재시작으로 카운터가 줄어든 구간은 새 카운터 값을
        그대로 더한다. 재시작이 없으면 결과는 `현재값 - 10분 전 값` 과 같다.
        """
        if not self._samples:
            return EvictionWindow(None, 0.0, False)

        samples = list(self._samples)
        now = samples[-1].ts
        start = now - self.window_seconds

        base_idx = 0
        for i, s in enumerate(samples):
            if s.ts <= start:
                base_idx = i
            else:
                break

        total, prev, first_ts, last_ts, restarted = 0, None, None, None, False
        for s in samples[base_idx:]:
            ns = s.nodes.get(addr)
            cur = None if ns is None else ns.evicted_total
            if cur is None:
                continue
            if prev is None:
                first_ts = s.ts
            else:
                if cur < prev:
                    restarted = True
                total += _delta(prev, cur)
            prev, last_ts = cur, s.ts

        if first_ts is None or last_ts is None:
            return EvictionWindow(None, 0.0, False)
        return EvictionWindow(total, last_ts - first_ts, restarted)


def _empty_series() -> dict[str, list]:
    return {"mem_pct": [], "evictions": [], "ops": [], "clients": []}


def _last_known_evicted(samples: list[Sample], addr: str) -> Optional[int]:
    for s in reversed(samples):
        ns = s.nodes.get(addr)
        if ns is not None and ns.evicted_total is not None:
            return ns.evicted_total
    return None


def _delta(prev: Optional[int], cur: Optional[int]) -> Optional[int]:
    if prev is None or cur is None:
        return None
    return cur - prev if cur >= prev else cur


def _round(v: Optional[float], nd: int) -> Optional[float]:
    return None if v is None else round(v, nd)
