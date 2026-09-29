"""响应时效面板服务端语义的无第三方依赖验证（python3 tests/test_latency.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在 SQLite 上
构造两轮完整演练事件流，调用 app.latency.build_latency_report 校验：

1. 逐卡逐组时延：首次回执/首次答复的虚拟毫秒差（揭示虚拟毫秒取载荷 revealed_at_ms）；
2. 撤销回执、修改答复不改首次响应时间（只取时间线上首个有效事件）；
3. 暂停时长不计入（暂停 100 墙钟秒，虚拟时间不推进）；
4. 揭示时刻目标组口径：全员卡取揭示瞬间的参与组（之后加入的组不计入），
   定向卡只取目标组；非目标组的回执/答复不计入；
5. 重置后从新一轮统计（旧轮数据保留在旧轮，新轮重新计数）；
6. 事件水位截断：水位之后的事件绝不混入；未响应为 None（未达），不用墙钟补足；
7. 中位数只取已响应样本，空样本为 None（无数据）；轮次越界报 LatencyError(404)。
"""
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import latency as lat  # noqa: E402

T0 = 1000.0


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(dbmod.SCHEMA)
    return conn


def add_event(conn, room_id, t, type_, payload):
    cur = conn.execute(
        "INSERT INTO events (room_id,type,payload,created_at) VALUES (?,?,?,?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), t))
    conn.commit()
    return cur.lastrowid


def revealed_payload(card_id, seq, title, group_id, revealed_at_ms):
    return {"id": card_id, "seq": seq, "trigger_seconds": 0, "title": title,
            "content": "", "group_id": group_id, "revealed": True,
            "revealed_at_ms": revealed_at_ms, "answers_locked": False}


def build_drill(conn):
    """构造两轮演练；返回 (room_id, 关键事件 id 字典, 组 id 元组, 卡片 id 元组)。"""
    rid = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
        " VALUES ('LAT001','时效','paused',0,NULL,?)", (T0,)).lastrowid
    g1 = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, "红队", "RED111", T0)).lastrowid
    g2 = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, "蓝队", "BLU222", T0)).lastrowid
    g3 = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, "替补队", "SUB333", T0)).lastrowid
    c1 = conn.execute(
        "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id) VALUES (?,?,?,?,?)",
        (rid, 1, 10, "告警风暴", None)).lastrowid
    c2 = conn.execute(
        "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id) VALUES (?,?,?,?,?)",
        (rid, 2, 20, "定向红队", g1)).lastrowid
    c3 = conn.execute(
        "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id) VALUES (?,?,?,?,?)",
        (rid, 3, 40, "二次告警", None)).lastrowid
    conn.commit()

    ev = {}
    ev["g1"] = add_event(conn, rid, T0 + 1, "group_added",
                         {"id": g1, "name": "红队", "join_code": "RED111"})
    add_event(conn, rid, T0 + 2, "group_added",
              {"id": g2, "name": "蓝队", "join_code": "BLU222"})

    # ---- 第 0 轮：开始于 T0+10（虚拟 0 起步）
    add_event(conn, rid, T0 + 10, "clock",
              {"action": "started", "status": "running", "elapsed_ms": 0})
    ev["rev_c1"] = add_event(conn, rid, T0 + 20, "card_revealed",
                             revealed_payload(c1, 1, "告警风暴", None, 10000))
    ev["ack1"] = add_event(conn, rid, T0 + 23, "receipt_updated",
                           {"card_id": c1, "group_id": g1, "acked": True})   # 虚拟 13s → +3000
    add_event(conn, rid, T0 + 25, "answer_updated",
              {"card_id": c1, "group_id": g1, "body": "v1", "revision": 1,
               "updated_at": T0 + 25})                                       # 虚拟 15s → +5000
    add_event(conn, rid, T0 + 26, "receipt_updated",
              {"card_id": c1, "group_id": g1, "acked": False})               # 撤销：不影响首次
    add_event(conn, rid, T0 + 27, "answer_updated",
              {"card_id": c1, "group_id": g1, "body": "v2", "revision": 2,
               "updated_at": T0 + 27})                                       # 修改：不影响首次
    add_event(conn, rid, T0 + 28, "receipt_updated",
              {"card_id": c1, "group_id": g1, "acked": True})                # 再回执：仍取 13s
    add_event(conn, rid, T0 + 29, "group_added",
              {"id": g3, "name": "替补队", "join_code": "SUB333"})           # 揭示后才加入
    add_event(conn, rid, T0 + 30, "clock",
              {"action": "paused", "status": "paused", "elapsed_ms": 20000})
    # 暂停 100 墙钟秒：虚拟时钟冻结在 20s
    add_event(conn, rid, T0 + 130, "clock",
              {"action": "resumed", "status": "running", "elapsed_ms": 20000})
    add_event(conn, rid, T0 + 135, "card_revealed",
              revealed_payload(c2, 2, "定向红队", g1, 25000))                # 虚拟 25s（暂停不计）
    add_event(conn, rid, T0 + 137, "receipt_updated",
              {"card_id": c2, "group_id": g1, "acked": True})                # 虚拟 27s → +2000
    add_event(conn, rid, T0 + 140, "card_revealed",
              revealed_payload(c3, 3, "二次告警", None, 30000))              # 目标组含替补队
    add_event(conn, rid, T0 + 141, "receipt_updated",
              {"card_id": c3, "group_id": g1, "acked": True})                # +1000
    add_event(conn, rid, T0 + 143, "answer_updated",
              {"card_id": c3, "group_id": g1, "body": "a", "revision": 1,
               "updated_at": T0 + 143})                                      # +3000
    add_event(conn, rid, T0 + 145, "receipt_updated",
              {"card_id": c3, "group_id": g3, "acked": True})                # +5000
    add_event(conn, rid, T0 + 146, "answer_updated",
              {"card_id": c3, "group_id": g3, "body": "a", "revision": 1,
               "updated_at": T0 + 146})                                      # +6000
    add_event(conn, rid, T0 + 147, "receipt_updated",
              {"card_id": c1, "group_id": g3, "acked": True})                # 非揭示时目标组：不计入
    ev["reset"] = add_event(conn, rid, T0 + 150, "reset",
                            {"action": "reset", "status": "idle", "elapsed_ms": 0})

    # ---- 第 1 轮：替补队已删除；同一卡片重新揭示，统计从零开始
    conn.execute("DELETE FROM groups WHERE id = ?", (g3,))
    conn.commit()
    add_event(conn, rid, T0 + 155, "group_removed", {"id": g3})
    add_event(conn, rid, T0 + 160, "clock",
              {"action": "started", "status": "running", "elapsed_ms": 0})
    add_event(conn, rid, T0 + 165, "card_revealed",
              revealed_payload(c1, 1, "告警风暴", None, 5000))               # 新一轮虚拟 5s
    add_event(conn, rid, T0 + 168, "receipt_updated",
              {"card_id": c1, "group_id": g2, "acked": True})                # +3000
    ev["last"] = add_event(conn, rid, T0 + 170, "answer_updated",
                           {"card_id": c1, "group_id": g2, "body": "b", "revision": 1,
                            "updated_at": T0 + 170})                          # +5000
    return rid, ev, (g1, g2, g3), (c1, c2, c3)


def group_of(report, gid):
    for g in report["groups"]:
        if g["group_id"] == gid:
            return g
    return None


def card_of(group, card_id):
    for c in group["cards"]:
        if c["card_id"] == card_id:
            return c
    return None


def test_round_zero_latencies():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, ev, (g1, g2, g3), (c1, c2, c3) = build_drill(conn)
        rep = lat.build_latency_report(conn, rid, 0, ev["last"], 12000)
        assert rep["round"] == 0 and rep["current_round"] == 1
        assert rep["high_water_event_id"] == ev["last"]
        assert [g["group_id"] for g in rep["groups"]] == [g1, g2, g3]

        r1 = group_of(rep, g1)
        assert [c["card_id"] for c in r1["cards"]] == [c1, c2, c3]  # 按揭示顺序
        k1 = card_of(r1, c1)
        assert k1["revealed_at_ms"] == 10000
        assert k1["receipt_latency_ms"] == 3000   # 撤销/再回执不改首次（13s）
        assert k1["answer_latency_ms"] == 5000    # 修改答复不改首次（15s）
        k2 = card_of(r1, c2)
        assert k2["card_group_id"] == g1 and k2["revealed_at_ms"] == 25000
        assert k2["receipt_latency_ms"] == 2000   # 暂停 100 墙钟秒不计入
        assert k2["answer_latency_ms"] is None    # 未答复：未达
        k3 = card_of(r1, c3)
        assert (k3["receipt_latency_ms"], k3["answer_latency_ms"]) == (1000, 3000)
        # 中位数只取已响应样本：回执 [3000,2000,1000] → 2000；答复 [5000,3000] → 4000
        assert r1["receipt_median_ms"] == 2000
        assert r1["answer_median_ms"] == 4000
        assert (r1["receipt_responded"], r1["answer_responded"]) == (3, 2)

        r2 = group_of(rep, g2)  # 蓝队全程未响应：未达 + 中位数无数据
        assert [c["card_id"] for c in r2["cards"]] == [c1, c3]      # 定向红队卡不在其列
        assert all(c["receipt_latency_ms"] is None and c["answer_latency_ms"] is None
                   for c in r2["cards"])
        assert r2["receipt_median_ms"] is None and r2["answer_median_ms"] is None
        assert (r2["receipt_responded"], r2["answer_responded"]) == (0, 0)

        r3 = group_of(rep, g3)  # 替补队：c1 揭示时还未加入（非目标组），其回执不计入
        assert [c["card_id"] for c in r3["cards"]] == [c3]
        k3 = card_of(r3, c3)
        assert (k3["receipt_latency_ms"], k3["answer_latency_ms"]) == (5000, 6000)
        assert r3["receipt_median_ms"] == 5000 and r3["answer_median_ms"] == 6000
        assert r3["exists"] is False               # 第 1 轮已被删除
        assert r3["group_name"] == "替补队"          # 已删组沿用事件流中的名字
        assert group_of(rep, g1)["exists"] is True
        conn.close()
    print("ok: round-zero per-card latencies, medians, pause exclusion, target-at-reveal")


def test_round_one_starts_fresh():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, ev, (g1, g2, g3), (c1, c2, c3) = build_drill(conn)
        rep = lat.build_latency_report(conn, rid, 1, ev["last"], 12000)
        assert rep["round"] == 1 and rep["current_round"] == 1
        # 替补队在本轮揭示前已删除，不再是任何卡的目标组
        assert [g["group_id"] for g in rep["groups"]] == [g1, g2]
        r2 = group_of(rep, g2)
        assert [c["card_id"] for c in r2["cards"]] == [c1]  # 只统计本轮揭示
        k1 = card_of(r2, c1)
        assert k1["revealed_at_ms"] == 5000
        assert (k1["receipt_latency_ms"], k1["answer_latency_ms"]) == (3000, 5000)
        assert r2["receipt_median_ms"] == 3000 and r2["answer_median_ms"] == 5000
        r1 = group_of(rep, g1)  # 本轮未响应：与上一轮数据互不影响
        assert card_of(r1, c1)["receipt_latency_ms"] is None
        assert r1["receipt_median_ms"] is None
        conn.close()
    print("ok: reset starts a fresh round; old rounds keep their own stats")


def test_high_water_truncation_and_unreached():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, ev, (g1, g2, g3), (c1, c2, c3) = build_drill(conn)
        # 水位取首次回执事件：回执已计入，其后的答复/新组/新揭示一律不混入
        rep = lat.build_latency_report(conn, rid, 0, ev["ack1"], 13000)
        assert rep["high_water_event_id"] == ev["ack1"]
        assert rep["current_round"] == 0
        r1 = group_of(rep, g1)
        assert [c["card_id"] for c in r1["cards"]] == [c1]
        k1 = card_of(r1, c1)
        assert k1["receipt_latency_ms"] == 3000
        assert k1["answer_latency_ms"] is None      # 答复事件在水位之后：未达
        assert group_of(rep, g3) is None            # 替补队加入事件在水位之后
        # 水位取揭示事件本身：回执尚未发生，显示未达（不用墙钟/当前时钟补足）
        rep0 = lat.build_latency_report(conn, rid, 0, ev["rev_c1"], 10000)
        k1 = card_of(group_of(rep0, g1), c1)
        assert k1["receipt_latency_ms"] is None and k1["answer_latency_ms"] is None
        assert group_of(rep0, g1)["receipt_median_ms"] is None
        conn.close()
    print("ok: event high-water truncation; unreached stays unreached")


def test_round_validation_and_empty_drill():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, ev, _, _ = build_drill(conn)
        for bad in (-1, 2, 99):
            try:
                lat.build_latency_report(conn, rid, bad, ev["last"], 0)
                raise AssertionError(f"round {bad} should be rejected")
            except lat.LatencyError as exc:
                assert exc.status_code == 404
        conn.close()

    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,created_at)"
            " VALUES ('LAT002','空演练','idle',0,1000.0)").lastrowid
        conn.commit()
        rep = lat.build_latency_report(conn, rid, 0, 0, 0)
        assert rep["round"] == 0 and rep["current_round"] == 0
        assert rep["groups"] == [] and rep["high_water_event_id"] == 0
        conn.close()
    print("ok: round validation (404) and empty drill report")


def test_median_samples_only():
    assert lat._median([]) is None
    assert lat._median([5]) == 5
    assert lat._median([3000, 1000, 2000]) == 2000
    assert lat._median([1000, 2001]) == 1500.5   # 偶数样本取中间两值平均
    assert lat._median([1000, 2000]) == 1500
    print("ok: medians take responded samples only; empty sample is None")


if __name__ == "__main__":
    test_round_zero_latencies()
    test_round_one_starts_fresh()
    test_high_water_truncation_and_unreached()
    test_round_validation_and_empty_drill()
    test_median_samples_only()
    print("ALL LATENCY TESTS PASSED")
