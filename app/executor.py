"""Redis 명령 실행기.

요건의 `redis-cli -p <port> <COMMAND>` 와 동일한 명령을 노드 단위로 실행하고
redis-cli 와 같은 '원문 텍스트' 를 돌려준다. 파싱은 parsers.py 가 담당하므로
어떤 실행기를 쓰든 결과 해석은 같다.

    redis-py  : 노드별 커넥션을 재사용한다. 운영 권장.
    redis-cli : 매 명령마다 redis-cli 프로세스를 띄운다. 서버에 redis-cli 가 있어야 한다.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Protocol

from .settings import MonitorConfig, NodeTarget

logger = logging.getLogger("pxa.redis_dashboard.executor")


class CommandError(Exception):
    """노드 연결 실패, 타임아웃, Redis 에러 응답을 하나로 묶는다."""


class Executor(Protocol):
    async def run(self, target: NodeTarget, *args: str) -> str: ...
    async def close(self) -> None: ...


class RedisPyExecutor:
    def __init__(self, cfg: MonitorConfig):
        import redis.asyncio as aioredis
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        self._cfg = cfg
        self._aioredis = aioredis
        self._retry = Retry(NoBackoff(), 0)      # 모니터링은 재시도 대신 다음 주기에 다시 본다
        self._clients: dict[str, "aioredis.Redis"] = {}

    def _client(self, t: NodeTarget):
        client = self._clients.get(t.addr)
        if client is None:
            timeout = self._cfg.command_timeout_seconds
            client = self._aioredis.Redis(
                host=t.host,
                port=t.port,
                username=self._cfg.username,
                password=self._cfg.password,
                decode_responses=True,
                socket_timeout=timeout,
                socket_connect_timeout=timeout,
                retry=self._retry,
                health_check_interval=0,
                client_name="redis-dashboard",
            )
            # 응답 파서를 끄고 redis-cli 와 동일한 원문 문자열을 받는다
            client.response_callbacks.clear()
            self._clients[t.addr] = client
        return client

    async def run(self, target: NodeTarget, *args: str) -> str:
        from redis.exceptions import RedisError

        try:
            value = await asyncio.wait_for(
                self._client(target).execute_command(*args),
                timeout=self._cfg.command_timeout_seconds,
            )
        except (RedisError, OSError, asyncio.TimeoutError) as e:
            raise CommandError(_describe(e)) from e
        return "" if value is None else str(value)

    async def close(self) -> None:
        for client in self._clients.values():
            try:
                await client.aclose()
            except Exception:  # 종료 중 오류는 무시
                pass
        self._clients.clear()


# redis-cli 는 에러 응답을 stdout 에 쓰고 exit 0 으로 끝나는 버전이 있어 본문으로도 판별한다
_CLI_ERROR = re.compile(
    r"^(ERR|NOAUTH|WRONGPASS|NOPERM|LOADING|BUSY|MASTERDOWN|READONLY|CLUSTERDOWN|MISCONF)\b"
)


class RedisCliExecutor:
    def __init__(self, cfg: MonitorConfig):
        self._cfg = cfg
        self._env = dict(os.environ)
        if cfg.password:
            # -a 옵션은 ps 에 비밀번호가 노출되므로 환경변수로 전달한다
            self._env["REDISCLI_AUTH"] = cfg.password

    async def run(self, target: NodeTarget, *args: str) -> str:
        cmd = [self._cfg.redis_cli_path, "-h", target.host, "-p", str(target.port)]
        if self._cfg.username:
            cmd += ["--user", self._cfg.username]
        cmd += list(args)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env,
            )
        except FileNotFoundError as e:
            raise CommandError(f"redis-cli 를 찾을 수 없습니다: {self._cfg.redis_cli_path}") from e

        try:
            out, err = await asyncio.wait_for(
                proc.communicate(), timeout=self._cfg.command_timeout_seconds
            )
        except asyncio.TimeoutError as e:
            proc.kill()
            await proc.wait()
            raise CommandError(f"{self._cfg.command_timeout_seconds}초 안에 응답이 없습니다") from e

        text = out.decode("utf-8", "replace").strip()
        err_text = err.decode("utf-8", "replace").strip()
        if proc.returncode != 0 or _CLI_ERROR.match(text):
            raise CommandError(err_text or text or f"redis-cli exit {proc.returncode}")
        return text

    async def close(self) -> None:
        return None


def create_executor(cfg: MonitorConfig) -> Executor:
    if cfg.executor == "redis-cli":
        logger.info("명령 실행기: redis-cli (%s)", cfg.redis_cli_path)
        return RedisCliExecutor(cfg)
    logger.info("명령 실행기: redis-py")
    return RedisPyExecutor(cfg)


def _describe(e: BaseException) -> str:
    if isinstance(e, asyncio.TimeoutError):
        return "응답 시간 초과"
    msg = str(e).strip()
    return msg or type(e).__name__
