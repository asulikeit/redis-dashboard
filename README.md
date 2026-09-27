# Redis Cluster Dashboard

Python 3.13 + FastAPI 0.128.0 + pxa-common 0.1.1 기반의 단일 페이지 Redis Cluster 모니터링 대시보드.

- 페이지는 `/` 하나. 인증 없음.
- 기본 10초마다 갱신, `http://localhost:8080/?refresh=30s` 로 변경 (`5s`, `30`, `1m` 형식 허용, 1초~10분)
- 최근 10분 추이는 **메모리 링버퍼**에 보관 (DB/sqlite 미사용, 재시작 시 추이는 다시 쌓임)
- 외부 CDN/폰트를 쓰지 않아 폐쇄망에서도 그대로 동작 (그래프는 SVG 로 직접 그림)

## 구조

```
redis-dashboard/
├── app/
│   ├── main.py        FastAPI 앱 조립 (pxa-common 로깅/예외핸들러/요청로그 미들웨어)
│   ├── __main__.py    python -m app 진입점 (server.host/port 사용)
│   ├── settings.py    config 섹션 모델 (pxa_common.load_section)
│   ├── executor.py    명령 실행기: redis-py(기본) / redis-cli(subprocess)
│   ├── parsers.py     INFO, CLUSTER INFO, CLUSTER NODES 파서
│   ├── collector.py   주기 수집 + 판정 + 화면용 스냅샷 생성
│   ├── store.py       10분 시계열 링버퍼, evicted_keys 10분 증가분 계산
│   ├── refresh.py     ?refresh= 파라미터 해석
│   ├── api.py         GET /, GET /api/snapshot, GET /healthz
│   └── static/index.html
├── config/
│   ├── config.yaml    대상 노드 등록
│   └── messages.yaml  사용자 노출 메시지 (pxa-common 메시지 카탈로그)
├── tests/test_core.py
└── requirements.txt
```

## 설치 및 실행

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 사내 Nexus 를 쓰는 경우
# pip install -r requirements.txt --index-url https://nexus.example.com/repository/pypi-internal/simple/

# config/config.yaml 의 monitor.nodes 를 실제 노드로 수정한 뒤
python -m app                                   # 0.0.0.0:8080
# 또는
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

비밀번호는 파일 대신 환경변수로 주입한다. (pxa-common 규칙: `PXA_<섹션>__<키>`)

```bash
export PXA_MONITOR__PASSWORD='...'
export PXA_CONFIG=/etc/redis-dashboard/config.yaml   # 다른 경로의 설정 파일을 쓸 때
```

## 대상 등록

클러스터는 하나만 모니터링하며, master/replica 를 **모두** `monitor.nodes` 에 등록한다.

```yaml
monitor:
  cluster_name: redis-prod
  nodes:                      # VM 3대 x (master 1 + replica 1) 예시
    - { host: 10.10.0.11, port: 7001 }
    - { host: 10.10.0.11, port: 7002 }
    - { host: 10.10.0.12, port: 7001 }
    - { host: 10.10.0.12, port: 7002 }
    - { host: 10.10.0.13, port: 7001 }
    - { host: 10.10.0.13, port: 7002 }
```

VM 1대 구성은 host 를 같게 두고 port 만 나열하면 된다. `CLUSTER NODES` 에는 보이는데 config 에 없는 노드가 있으면 Nodes 항목이 `주의` 로 표시된다.

## 모니터링 항목과 수집 방식

| # | 항목 | 명령 | 화면 | 판정 |
|---|------|------|------|------|
| 1 | Cluster State | `CLUSTER INFO` → `cluster_state` | 클러스터 점검 표 | 응답한 모든 노드가 `ok` 여야 정상 |
| 2 | Nodes | `CLUSTER NODES` | 클러스터 점검 표 | `fail`, `noaddr` → 이상 / `fail?`, `handshake`, config 미등록 노드 → 주의 |
| 3 | Slots | `CLUSTER INFO` → `cluster_slots_assigned`, `cluster_slots_ok` | 클러스터 점검 표 | 둘 다 16384 |
| 4 | Fail/PFail | `CLUSTER INFO` → `cluster_slots_fail`, `cluster_slots_pfail` | 클러스터 점검 표 | fail>0 이상, pfail>0 주의 |
| 5 | DBSize | master 에서만 `DBSIZE` | 노드 표 | - |
| 6 | Memory % | `INFO memory` → `used_memory / maxmemory × 100` | 노드 표 + **그래프** | 80% 주의, 90% 이상 (설정 가능) |
| 7 | Evicted Keys 10분 | `INFO stats` → `evicted_keys` | 노드 표 + **그래프** | 10분 증가분 > 0 이면 주의 |
| 8 | 상태/Role | `PING` = `PONG`, `INFO replication` → `role` | 노드 표 | PONG 이 아니면 해당 노드 응답 없음 |
| - | OPS/sec | `INFO stats` → `instantaneous_ops_per_sec` | **그래프** + 노드 표 | - |
| - | Clients | `INFO clients` → `connected_clients` | **그래프** + 노드 표 | - |

세부 동작:

- **노드마다 보는 값이 다를 수 있는 항목**(1, 3, 4)은 응답한 모든 노드의 `CLUSTER INFO` 를 받아 **가장 나쁜 값**을 표시한다. 네트워크 분리 시 소수 쪽 노드만 `fail` 을 보는 경우를 놓치지 않기 위함이다.
- **`fail?`(pfail)은 관찰한 노드에게만 보이는 플래그**이므로 2번은 모든 노드의 `CLUSTER NODES` 를 합쳐서 판단하고, 몇 개 노드가 관측했는지 함께 보여준다.
- **7번 10분 eviction** 은 "10분 전 샘플(윈도우 시작 이전의 마지막 샘플)" 과 현재 샘플의 차이다. 중간에 노드 재시작/`CONFIG RESETSTAT` 으로 카운터가 줄면 그 구간은 새 카운터 값을 더하고 `카운터 초기화 감지` 로 표시한다. 기동 후 10분이 안 됐으면 `수집 N분 기준` 으로 표시한다.
- **maxmemory 가 0(무제한)** 이면 `memory_basis_when_unlimited: system` 설정 시 `total_system_memory` 기준으로 계산하고 화면에 `시스템 메모리 기준` 을 표시한다. `none` 이면 계산하지 않는다.

## 수집 주기와 화면 갱신 주기

- 서버는 `collect_interval_seconds`(기본 10초)마다 백그라운드에서 한 번 수집하고 결과를 메모리에 둔다.
- 브라우저는 `?refresh=` 주기로 `/api/snapshot` 만 읽는다. 따라서 접속자 수나 refresh 값과 무관하게 Redis 에 가는 명령 수는 일정하다.
- refresh 를 수집 주기보다 짧게(예: 5s) 잡으면 같은 데이터를 다시 받을 뿐이다. 더 촘촘한 추이가 필요하면 `collect_interval_seconds` 를 줄인다.
- 페이지 상단 가는 선은 다음 갱신까지 남은 시간이다. 수집이 `max(3 × 수집주기, 30초)` 이상 멈추면 경고를 띄운다.

## 명령 실행 방식

| executor | 설명 |
|----------|------|
| `redis-py` (기본) | 노드별 커넥션을 재사용해 `PING`, `INFO ...`, `CLUSTER ...` 를 그대로 전송. 응답 파서를 끄고 redis-cli 와 같은 원문 텍스트를 받아 같은 파서로 해석 |
| `redis-cli` | `redis-cli -h <host> -p <port> <COMMAND>` 를 subprocess 로 실행. 요건의 명령과 1:1 로 맞추고 싶을 때. 비밀번호는 `-a` 대신 `REDISCLI_AUTH` 환경변수로 전달(ps 노출 방지) |

한 주기에 노드당 7~8개 명령(`PING`, `INFO` 4종, `CLUSTER INFO`, `CLUSTER NODES`, master 는 `DBSIZE`)을 병렬로 보낸다. 모두 O(1) 또는 노드 수에 비례하는 가벼운 명령이다.

## API

`GET /api/snapshot` — pxa-common 표준 응답 포맷

```json
{ "success": true, "code": "pxa-10000", "message": "정상 처리되었습니다.",
  "result": { "verdict": {...}, "checks": [...], "nodes": [...], "series": {"ts": [...], "nodes": {...}} } }
```

기동 직후 첫 수집 전에는 `result: null` 과 안내 메시지를 돌려준다.

## 테스트

```bash
pip install pytest
pytest -q
```
