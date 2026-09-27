import os
from pathlib import Path

os.environ.setdefault("PXA_CONFIG", str(Path(__file__).resolve().parent.parent / "config" / "config.yaml"))

from pxa_common import load_message_files  # noqa: E402

from app.parsers import parse_cluster_nodes, parse_kv  # noqa: E402
from app.refresh import resolve_refresh  # noqa: E402
from app.store import MetricStore, NodeSample, Sample  # noqa: E402

load_message_files([str(Path(__file__).resolve().parent.parent / "config" / "messages.yaml")])

CLUSTER_NODES = """\
07c3 10.0.0.11:7001@17001 myself,master - 0 0 1 connected 0-5460
67ed 10.0.0.12:7001@17001,redis-b master - 0 0 2 connected 5461-10922
292f 10.0.0.13:7001@17001 master,fail? - 0 0 3 connected 10923-16383
6ec2 10.0.0.12:7002@17002 slave 07c3 0 0 1 connected
824f 10.0.0.13:7002@17002 slave,fail 67ed 0 0 2 disconnected
e7d1 :0@0 handshake,noaddr - 0 0 0 disconnected
"""


def test_parse_kv_skips_sections_and_blank():
    kv = parse_kv("# Memory\r\nused_memory:100\r\nmaxmemory:0\r\n\r\n")
    assert kv == {"used_memory": "100", "maxmemory": "0"}


def test_parse_cluster_nodes_flags_and_addr():
    entries = parse_cluster_nodes(CLUSTER_NODES)
    assert len(entries) == 6
    assert entries[0].is_myself and entries[0].is_master
    assert entries[1].addr == "10.0.0.12:7001"            # hostname 부분 제거
    assert entries[2].problem_flags == ["fail?"]
    assert entries[4].problem_flags == ["fail"]
    assert entries[4].master_id == "67ed"
    assert set(entries[5].problem_flags) == {"handshake", "noaddr"}


def _store_with(values, interval=10, window=600, addr="n1"):
    st = MetricStore(window, interval)
    for i, v in enumerate(values):
        st.add(Sample(ts=1000 + i * interval, nodes={addr: NodeSample(1.0, v, 1, 1)}))
    return st


def test_evictions_is_current_minus_10min_ago():
    # 0..70 샘플(700초) -> 기준은 now-600 이하 마지막 샘플
    values = [i * 5 for i in range(71)]
    ev = _store_with(values).evictions_in_window("n1")
    assert ev.count == values[-1] - values[10]
    assert ev.covered_seconds == 600
    assert not ev.restarted


def test_evictions_partial_window_after_startup():
    ev = _store_with([100, 103, 110]).evictions_in_window("n1")
    assert ev.count == 10 and ev.covered_seconds == 20


def test_evictions_counter_reset():
    ev = _store_with([100, 120, 3, 8]).evictions_in_window("n1")
    assert ev.count == 20 + 3 + 5
    assert ev.restarted


def test_evictions_skips_gaps_when_node_down():
    st = MetricStore(600, 10)
    st.add(Sample(0, {"n1": NodeSample(1, 10, 1, 1)}))
    st.add(Sample(10, {}))                                  # 노드 다운
    st.add(Sample(20, {"n1": NodeSample(1, 15, 1, 1)}))
    assert st.evictions_in_window("n1").count == 5
    series = st.series(["n1"])
    assert series["nodes"]["n1"]["evictions"] == [None, None, 5]
    assert series["nodes"]["n1"]["mem_pct"] == [1, None, 1]


def test_store_prunes_old_samples():
    st = _store_with(list(range(200)))
    assert len(st) <= 600 // 10 + 3
    assert len(st.series(["n1"])["ts"]) == 61               # 0분 ~ 10분 양끝 포함


def test_refresh_parsing():
    assert resolve_refresh(None, "10s").seconds == 10
    assert resolve_refresh("30s", "10s").seconds == 30
    assert resolve_refresh("30", "10s").seconds == 30
    assert resolve_refresh("1m", "10s").seconds == 60
    bad = resolve_refresh("abc", "10s")
    assert bad.seconds == 10 and "abc" in bad.notice
    clamped = resolve_refresh("2h", "10s")
    assert clamped.seconds == 10 and clamped.notice      # 해석 불가 단위
    big = resolve_refresh("3600s", "10s")
    assert big.seconds == 600 and big.notice
