"""FastAPI 앱 조립.

    uvicorn app.main:app --host 0.0.0.0 --port 8080
    python -m app            # config 의 server.host/port 사용
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
# 어느 디렉토리에서 실행해도 프로젝트의 config 를 쓰도록 (PXA_CONFIG 가 있으면 그 값 우선)
os.environ.setdefault("PXA_CONFIG", str(BASE_DIR / "config" / "config.yaml"))

from pxa_common import (  # noqa: E402  (PXA_CONFIG 지정 후 import)
    RequestLoggingMiddleware,
    get_app_config,
    get_logging_config,
    get_messages_config,
    load_message_files,
    register_exception_handlers,
    setup_logging,
)
from pxa_common.fastapi import FastAPI  # noqa: E402

from .api import router  # noqa: E402
from .collector import Collector  # noqa: E402
from .executor import create_executor  # noqa: E402
from .settings import get_monitor_config  # noqa: E402
from .store import MetricStore  # noqa: E402

logger = logging.getLogger("pxa.redis_dashboard")


def _project_path(p: str) -> str:
    path = Path(p)
    return str(path if path.is_absolute() else BASE_DIR / path)


def create_app() -> FastAPI:
    log_cfg = get_logging_config()
    log_cfg.dir = _project_path(log_cfg.dir)
    setup_logging(log_cfg)
    load_message_files(_project_path(f) for f in get_messages_config().files)

    monitor_cfg = get_monitor_config()
    app_cfg = get_app_config()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = MetricStore(monitor_cfg.window_seconds, monitor_cfg.collect_interval_seconds)
        collector = Collector(monitor_cfg, create_executor(monitor_cfg), store)
        app.state.collector = collector
        logger.info(
            "수집 시작: cluster=%s nodes=%s interval=%ss window=%sm",
            monitor_cfg.cluster_name,
            ", ".join(n.addr for n in monitor_cfg.nodes),
            monitor_cfg.collect_interval_seconds,
            monitor_cfg.window_minutes,
        )
        collector.start()
        try:
            yield
        finally:
            await collector.stop()
            logger.info("수집 종료")

    app = FastAPI(
        title=app_cfg.name,
        debug=app_cfg.debug,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.monitor_config = monitor_cfg
    app.add_middleware(RequestLoggingMiddleware)
    register_exception_handlers(app)
    app.include_router(router)
    return app


app = create_app()
