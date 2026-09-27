"""`?refresh=30s` 파라미터 해석."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from pxa_common import msg

MIN_SECONDS = 1
MAX_SECONDS = 600          # 추이 윈도우(10분)보다 길게 갱신할 이유가 없다

_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|sec|m|min)?\s*$", re.IGNORECASE)
_UNIT = {None: 1, "s": 1, "sec": 1, "m": 60, "min": 60, "ms": 0.001}


@dataclass(frozen=True)
class Refresh:
    seconds: int
    notice: Optional[str] = None


def parse_duration(value: str) -> Optional[float]:
    """'30s' '30' '1m' '1500ms' -> 초. 해석 불가면 None."""
    m = _PATTERN.match(value or "")
    if not m:
        return None
    unit = (m.group(2) or "").lower() or None
    return float(m.group(1)) * _UNIT[unit]


def resolve_refresh(requested: Optional[str], default: str) -> Refresh:
    default_seconds = int(round(parse_duration(default) or 10))
    if requested is None or requested.strip() == "":
        return Refresh(default_seconds)

    seconds = parse_duration(requested)
    if seconds is None:
        return Refresh(
            default_seconds,
            msg("dashboard.refresh_invalid", value=requested, fallback=default_seconds),
        )

    applied = min(MAX_SECONDS, max(MIN_SECONDS, int(round(seconds))))
    if applied != seconds:
        if MIN_SECONDS <= seconds <= MAX_SECONDS:     # 1.5s 같은 반올림은 조용히 처리
            return Refresh(applied)
        return Refresh(
            applied,
            msg("dashboard.refresh_clamped", min=MIN_SECONDS, max=MAX_SECONDS, applied=applied),
        )
    return Refresh(applied)
