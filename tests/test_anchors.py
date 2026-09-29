"""复盘锚点服务端语义的无第三方依赖验证（python3 tests/test_anchors.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在 SQLite 上
复刻服务端在 state_lock 临界区内对锚点的操作次序与建表结构，校验：

1. 绑定次序：先轮次（reset 计数）→ 再水位（MAX events.id）→ 再虚拟毫秒，
   锚点自身创建事件 id 恰为「绑定水位 + 1」，created_event_id 回填正确；
2. 暂停冻结：paused/idle 时锚点 vtime_ms 取冻结值，不因墙钟推进而变化；
3. 跨重置归属：reset 不删锚点，新锚点 round +1，旧锚点仍属原轮次；
4. 导出截断：只含 created_event_id <= high_water 的锚点，回放截断水位 event_id
   严格小于其自身事件 id（同刻更晚事件天然被排除）；
5. 事件顺序与视野白名单口径（anchor_* 不在 export 白名单）。
"""
import json
import os
import sqlite3
import sys
import tempfile
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import export as rv  # noqa: E402


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


def current_round(conn, room_id):
    # 与 main.current_round 同一口径
    return conn.execute(
        "SELECT COUNT(*) c FROM events WHERE room_id=? AND type='reset'", (room_id,)).fetchone()["c"]


def high_water(conn, room_id):
    return conn.execute(
        "SELECT COALESCE(MAX(id),0) m FROM events WHERE room_id=?", (room_id,)).fetchone()["m"]


def create_anchor(conn, room_id, title, note, vtime_ms, now):
    """复刻 main.create_anchor 临界区内的操作次序（无广播，事件仍落库）。"""
    round_idx = current_round(conn, room_id)
    bound = high_water(conn, room_id)
    cur = conn.execute(
        "INSERT INTO anchors (room_id,title,note,round,vtime_ms,event_id,created_event_id,"
        "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (room_id, title, note, round_idx, vtime_ms, bound, bound, now, now))
    anchor_id = cur.lastrowid
    conn.commit()
    eid = add_event(conn, room_id, now, "anchor_created",
                    {"id": anchor_id, "round": round_idx, "vtime_ms": vtime_ms, "event_id": bound})
    assert eid == bound + 1
    conn.execute("UPDATE anchors SET created_event_id=? WHERE id=?", (eid, anchor_id))
    conn.commit()
    return conn.execute("SELECT * FROM anchors WHERE id=?", (anchor_id,)).fetchone()


def test_binding_order_and_freeze():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        now = time.time()
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('ANC001','锚点','idle',0,NULL,?)", (now,)).lastrowid
        conn.commit()

        # idle 状态即可记录：vtime=0，绑定水位 0，自身事件 #1
        a0 = create_anchor(conn, rid, "待机锚点", "", 0, now + 1)
        assert a0["round"] == 0 and a0["vtime_ms"] == 0
        assert a0["event_id"] == 0 and a0["created_event_id"] == 1

        # 开始 → 暂停在 12s；之后墙钟流逝再多，锚点虚拟时间仍冻结 12s
        e_start = add_event(conn, rid, now + 2, "clock",
                            {"action": "started", "status": "running", "elapsed_ms": 0})
        conn.execute(
            "UPDATE rooms SET status='paused',elapsed_ms=12000,last_resume_at=NULL WHERE id=?", (rid,))
        conn.commit()
        e_pause = add_event(conn, rid, now + 20, "clock",
                            {"action": "paused", "status": "paused", "elapsed_ms": 12000})
        a1 = create_anchor(conn, rid, "暂停中", "时间应冻结", 12000, now + 100)
        assert a1["vtime_ms"] == 12000 and a1["round"] == 0
        assert a1["event_id"] == e_pause  # 绑定到暂停事件水位

        # 暂停期间记录第二个锚点（墙钟更晚）：vtime 仍是冻结值 12000，水位推进
        a2 = create_anchor(conn, rid, "暂停中第二个", "", 12000, now + 200)
        assert a2["vtime_ms"] == 12000
        assert a2["event_id"] == a1["created_event_id"]
        assert a2["created_event_id"] == a1["created_event_id"] + 1

        # 同虚拟毫秒内：锚点事件自身 id 更大，回放截断到 event_id 时它不计入
        assert a1["event_id"] < a1["created_event_id"]
        assert a2["event_id"] < a2["created_event_id"]
        conn.close()
    print("ok: anchor binding order and paused-time freeze")


def test_reset_keeps_anchors_and_round_ownership():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        now = time.time()
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,created_at)"
            " VALUES ('ANC002','重置','paused',30000,?)", (now,)).lastrowid
        conn.commit()
        add_event(conn, rid, now, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})
        e_pause = add_event(conn, rid, now + 30, "clock",
                            {"action": "paused", "status": "paused", "elapsed_ms": 30000})
        a1 = create_anchor(conn, rid, "第一轮", "x", 30000, now + 31)

        # 重置：复刻 reset 路由（不清 anchors）
        conn.execute(
            "UPDATE rooms SET status='idle',elapsed_ms=0,last_resume_at=NULL WHERE id=?", (rid,))
        conn.execute("UPDATE cards SET revealed=0, revealed_at_ms=NULL, timed_out=0,"
                     " answers_locked=0 WHERE room_id=?", (rid,))
        conn.execute("DELETE FROM receipts WHERE room_id=?", (rid,))
        conn.execute("DELETE FROM answers WHERE room_id=?", (rid,))
        conn.commit()
        e_reset = add_event(conn, rid, now + 40, "reset",
                            {"action": "reset", "status": "idle", "elapsed_ms": 0})

        a2 = create_anchor(conn, rid, "第二轮", "y", 0, now + 41)
        assert a2["round"] == 1 and a2["vtime_ms"] == 0
        # 旧锚点仍归属原轮次，未被重置删除
        old = conn.execute("SELECT * FROM anchors WHERE id=?", (a1["id"],)).fetchone()
        assert old["round"] == 0 and old["vtime_ms"] == 30000
        assert conn.execute("SELECT COUNT(*) c FROM anchors").fetchone()["c"] == 2

        # 完整导出：两个锚点都在，按 (round, vtime) 排序
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        hw = high_water(conn, rid)
        data = rv.build_review_data(conn, room, hw)
        assert [(a["round"], a["vtime_ms"]) for a in data["anchors"]] == [(0, 30000), (1, 0)]

        # 水位取 e_reset（第二轮锚点尚未创建）：只含第一轮锚点
        data0 = rv.build_review_data(conn, room, e_reset)
        assert [a["id"] for a in data0["anchors"]] == [a1["id"]]
        conn.close()
    print("ok: anchors survive reset with original round ownership")


def test_replay_cap_is_strict_same_vtime_boundary():
    """精确还原：锚点绑定水位的下一事件即使同虚拟毫秒，也被排除在回放截断之外。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        now = 1000.0
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('ANC003','同刻','running',0,?,?)", (now, now)).lastrowid
        conn.commit()
        g = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, "G", "GGG111", now)).lastrowid
        c = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id) VALUES (?,?,?,?,?)",
            (rid, 1, 10, "卡", None)).lastrowid
        conn.commit()
        add_event(conn, rid, now, "group_added", {"id": g, "name": "G", "join_code": "GGG111"})
        e_start = add_event(conn, rid, now, "clock",
                            {"action": "started", "status": "running", "elapsed_ms": 0})
        conn.execute("UPDATE cards SET revealed=1, revealed_at_ms=10000 WHERE id=?", (c,))
        conn.commit()
        # 同一墙钟（虚拟 10s）：揭示事件
        e_rev = add_event(conn, rid, now + 10, "card_revealed",
                          {"id": c, "seq": 1, "title": "卡", "group_id": None,
                           "revealed": True, "revealed_at_ms": 10000,
                           "timed_out": False, "answers_locked": False})
        # 紧接着（同虚拟 10s，墙钟 +10 整）红队回执
        e_ack = add_event(conn, rid, now + 10, "receipt_updated",
                          {"card_id": c, "group_id": g, "acked": True})
        conn.execute(
            "INSERT INTO receipts (room_id,card_id,group_id,created_at) VALUES (?,?,?,?)",
            (rid, c, g, now + 10))
        conn.commit()
        # 主持人在回执之后记录锚点：绑定水位含回执；此处构造一个「揭示后、回执前」
        # 不可能由真实 UI 直接产生的锚点，手工置绑定水位 = e_rev，验证截断严格性
        a = create_anchor(conn, rid, "只要揭示不要回执", "", 10000, now + 10)
        conn.execute("UPDATE anchors SET event_id=?, created_event_id=? WHERE id=?",
                     (e_rev, a["created_event_id"], a["id"]))
        conn.commit()

        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        hw = high_water(conn, rid)
        data = rv.build_review_data(conn, room, hw)
        anchor = next(x for x in data["anchors"] if x["id"] == a["id"])
        assert anchor["event_id"] == e_rev
        # 回放页的过滤条件 e.id <= anchor.event_id：
        # 同刻的回执事件 e_ack（id = e_rev+1）必须被排除
        evs = data["events"]
        kept = [e for e in evs if e["round"] == 0 and e["vtime_ms"] <= 10000
                and e["id"] <= anchor["event_id"]]
        types = [e["type"] for e in kept]
        assert "card_revealed" in types and "receipt_updated" not in types, types

        # 锚点事件本身不脱敏泄露进事件流
        assert all(not e["type"].startswith("anchor_") for e in evs)

        blob = rv.build_review_zip(data)
        with zipfile.ZipFile(__import__("io").BytesIO(blob)) as zf:
            html = zf.read("replay.html").decode("utf-8")
            assert "只要揭示不要回执" in html and "__REPLAY_DATA_JSON__" not in html
        conn.close()
    print("ok: anchor replay cap strictly excludes later same-vtime events")


if __name__ == "__main__":
    test_binding_order_and_freeze()
    test_reset_keeps_anchors_and_round_ownership()
    test_replay_cap_is_strict_same_vtime_boundary()
    print("ALL ANCHOR TESTS PASSED")
