"""라우터: 대시보드 페이지 1개 + 스냅샷 API."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from pxa_common import ApiResponse, msg
from pxa_common.fastapi import APIRouter, HTMLResponse, Query, Request

from .refresh import resolve_refresh

router = APIRouter()

_TEMPLATE = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def dashboard(
    request: Request,
    refresh: Optional[str] = Query(None, description="갱신 주기. 예: 10s, 30s, 1m"),
) -> HTMLResponse:
    cfg = request.app.state.monitor_config
    r = resolve_refresh(refresh, cfg.default_refresh)
    boot = {
        "refreshSeconds": r.seconds,
        "notice": r.notice,
        "clusterName": cfg.cluster_name,
        "windowSeconds": cfg.window_seconds,
        "snapshotUrl": str(request.url_for("snapshot")),
    }
    # </script> 조기 종료 방지
    boot_json = json.dumps(boot, ensure_ascii=False).replace("</", "<\\/")
    html = _TEMPLATE.replace("__BOOT_JSON__", boot_json).replace(
        "__CLUSTER_NAME__", _escape(cfg.cluster_name)
    )
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})


@router.get("/api/snapshot", name="snapshot")
async def snapshot(request: Request) -> ApiResponse:
    collector = request.app.state.collector
    if collector.snapshot is None:
        return ApiResponse.ok(
            result=None,
            message=msg("dashboard.warming_up", interval=collector.cfg.collect_interval_seconds),
        )
    return ApiResponse.ok(result=collector.snapshot)


@router.get("/healthz", include_in_schema=False)
async def healthz() -> ApiResponse:
    return ApiResponse.ok(result={"status": "up"})


def _escape(s: str) -> str:
    return (
        s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        .replace('"', "&quot;").replace("'", "&#39;")
    )
