"""启动恢复冻结时钟并补 paused 事件的无第三方依赖验证（python3 tests/test_restart_recovery.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），直接在 app.db 建表的
SQLite 上复刻「进程在房间运行中停止 → 重新连接并执行 init_schema」的真实恢复
路径，校验：

1. 仅停机时仍 running 的房间被冻结为 paused，并在同一事务追加一条现有格式的
   paused 时钟事件：elapsed_ms 与房间冻结值一致，created_at 取本次恢复时刻，
   结算沿用既有公式（冻结值 + now − last_resume_at）；
2. 重复初始化不重复追加；原本 idle/paused 的房间不追加任何事件；
3. 导出/时效面板据此冻结：恢复后（暂停期间）提交的回执/答复 vtime 挂在冻结值，
   暂停墙钟时长不计入响应时延；
4. 继续运行后从冻结值累计；跨 reset 仍按轮次隔离（恢复冻结事件归属停机前轮次）。
"""
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import export as rv  # noqa: E402
from app import latency as lat  # noqa: E402


def fresh_conn(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def open_db(path):
    conn = fresh_conn(path)
    conn.executescript(dbmod.SCHEMA)
    return conn


def add_event(conn, room_id, t, type_, payload):
    cur = conn.execute(
        "INSERT INTO events (room_id,type,payload,created_at) VALUES (?,?,?,?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), t))
    conn.commit()
    return cur.lastrowid


def paused_events(conn, room_id):
    return conn.execute(
        "SELECT * FROM events WHERE room_id=? AND type='clock'"
        " AND json_extract(payload,'$.action')='paused' ORDER BY id",
        (room_id,)).fetchall()


def test_running_room_frozen_with_paused_event():
    """running 房间恢复时：冻结房间并补一条 elapsed_ms 一致、created_at=恢复时刻的 paused 事件。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        t0 = time.time() - 90  # 假装 90 秒前进入 running，elapsed_ms 基数 30s
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV001','重启','running',30000,?,?)", (t0, t0)).lastrowid
        conn.commit()
        conn.close()

        conn = fresh_conn(path)
        recover = time.time()
        dbmod.init_schema(conn)
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        assert room["status"] == "paused"
        assert room["last_resume_at"] is None
        # 沿用既有结算公式：30s 冻结值 + 约 90s 本次运行段
        assert room["elapsed_ms"] >= 120_000
        assert room["elapsed_ms"] < 125_000
        frozen = room["elapsed_ms"]

        events = paused_events(conn, rid)
        assert len(events) == 1, f"应恰好补一条恢复 paused 事件，实际 {len(events)}"
        ev = events[0]
        payload = json.loads(ev["payload"])
        assert payload == {"action": "paused", "status": "paused", "elapsed_ms": frozen}
        assert ev["created_at"] >= recover - 1  # created_at 取本次恢复时刻
        conn.close()
    print("ok: restart freezes running room and appends matching paused clock event")


def test_reinit_idempotent_and_idle_paused_untouched():
    """重复初始化不重复追加；原本 idle/paused 的房间不追加。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        t0 = time.time() - 30
        rid_run = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV010','运行','running',0,?,?)", (t0, t0)).lastrowid
        rid_idle = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,created_at) VALUES ('RCV011','空闲','idle',0,?)",
            (t0,)).lastrowid
        rid_paused = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,created_at) VALUES ('RCV012','暂停','paused',5000,?)",
            (t0,)).lastrowid
        conn.commit()
        conn.close()

        conn = fresh_conn(path)
        dbmod.init_schema(conn)
        assert len(paused_events(conn, rid_run)) == 1
        # 再初始化一次：running 房间已被冻结为 paused，不得重复追加
        dbmod.init_schema(conn)
        assert len(paused_events(conn, rid_run)) == 1
        for rid in (rid_idle, rid_paused):
            assert conn.execute("SELECT COUNT(*) c FROM events WHERE room_id=?",
                                (rid,)).fetchone()["c"] == 0
        # idle/paused 房间状态也保持原样
        assert conn.execute("SELECT status FROM rooms WHERE id=?", (rid_idle,)).fetchone()["status"] == "idle"
        paused_room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid_paused,)).fetchone()
        assert paused_room["status"] == "paused" and paused_room["elapsed_ms"] == 5000
        conn.close()
    print("ok: re-init does not duplicate; idle/paused rooms untouched")


def _make_running_drill_before_crash(path):
    """构造：开始（虚拟 0）→ 10s 揭示全员卡 → 进程在运行中停止。返回 (conn, rid, gid, cid)。"""
    conn = open_db(path)
    t0 = time.time() - 200
    rid = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
        " VALUES ('RCV020','演练','running',0,?,?)", (t0, t0)).lastrowid
    gid = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, "红队", "RED020", t0)).lastrowid
    cid = conn.execute(
        "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id) VALUES (?,?,?,?,?)",
        (rid, 1, 10, "告警", None)).lastrowid
    conn.commit()
    add_event(conn, rid, t0 - 1, "group_added", {"id": gid, "name": "红队"})
    add_event(conn, rid, t0, "clock",
              {"action": "started", "status": "running", "elapsed_ms": 0})
    add_event(conn, rid, t0 + 10, "card_revealed",
              {"id": cid, "seq": 1, "trigger_seconds": 10, "title": "告警",
               "group_id": None, "revealed": True, "revealed_at_ms": 10000,
               "answers_locked": False})
    conn.commit()
    conn.close()
    return rid, gid, cid


def test_pause_after_restart_does_not_inflate_latency():
    """恢复后（暂停中）过再久才回执/答复，vtime 与时延都只取冻结值。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        rid, gid, cid = _make_running_drill_before_crash(path)

        conn = fresh_conn(path)
        dbmod.init_schema(conn)  # running → paused + 恢复 paused 事件
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        frozen = room["elapsed_ms"]
        assert room["status"] == "paused"

        # 恢复后又过了 60 墙钟秒（暂停期间），红队才回执、再答复
        recover = paused_events(conn, rid)[0]["created_at"]
        later = recover + 60
        conn.execute(
            "INSERT INTO receipts (room_id,card_id,group_id,created_at) VALUES (?,?,?,?)",
            (rid, cid, gid, later))
        conn.commit()
        add_event(conn, rid, later, "receipt_updated",
                  {"card_id": cid, "group_id": gid, "acked": True})
        add_event(conn, rid, later + 5, "answer_updated",
                  {"card_id": cid, "group_id": gid, "body": "处置", "revision": 1,
                   "updated_at": later + 5})
        high = conn.execute("SELECT MAX(id) m FROM events WHERE room_id=?",
                            (rid,)).fetchone()["m"]

        # 导出时间线：回执/答复 vtime 挂冻结值，不含恢复后暂停的 60s
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        data = rv.build_review_data(conn, room, high)
        vtime = {e["type"]: e["vtime_ms"] for e in data["events"]}
        assert vtime["receipt_updated"] == frozen
        assert vtime["answer_updated"] == frozen
        assert ("paused", frozen) in [(a["action"], a["vtime_ms"])
                                      for a in data["clock_actions"]]

        # 响应时延 = 冻结值 − 揭示 10s，恢复后暂停的 60s 绝不计入
        rep = lat.build_latency_report(conn, rid, 0, high, frozen)
        card = next(c for g in rep["groups"] if g["group_id"] == gid for c in g["cards"])
        assert card["receipt_latency_ms"] == frozen - 10000
        assert card["answer_latency_ms"] == frozen - 10000
        conn.close()
    print("ok: post-restart pause keeps receipts/answers at frozen vtime; latency not inflated")


def test_resume_accumulates_from_frozen_and_reset_isolates_rounds():
    """继续运行后从冻结值累计；跨重置恢复冻结事件仍归属停机前轮次。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        rid, gid, cid = _make_running_drill_before_crash(path)

        conn = fresh_conn(path)
        dbmod.init_schema(conn)
        frozen = conn.execute("SELECT elapsed_ms FROM rooms WHERE id=?",
                              (rid,)).fetchone()["elapsed_ms"]
        recover = paused_events(conn, rid)[0]["created_at"]

        # 暂停态直接重置（合法），第二轮重新开始
        reset_wall = recover + 30
        conn.execute(
            "UPDATE rooms SET status='idle', elapsed_ms=0, last_resume_at=NULL WHERE id=?",
            (rid,))
        conn.commit()
        add_event(conn, rid, reset_wall, "reset",
                  {"action": "reset", "status": "idle", "elapsed_ms": 0})
        w2 = reset_wall + 10
        conn.execute(
            "UPDATE rooms SET status='running', elapsed_ms=0, last_resume_at=? WHERE id=?",
            (w2, rid))
        conn.commit()
        add_event(conn, rid, w2, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})
        add_event(conn, rid, w2 + 7, "card_revealed",
                  {"id": cid, "seq": 1, "title": "告警", "group_id": None,
                   "revealed_at_ms": 7000})
        high = conn.execute("SELECT MAX(id) m FROM events WHERE room_id=?",
                            (rid,)).fetchone()["m"]
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        data = rv.build_review_data(conn, room, high)

        # 恢复 paused 事件属于第 0 轮；第二轮独立计数，揭示 vtime=7000
        assert ("paused", 0) in [(a["action"], a["round"]) for a in data["clock_actions"]]
        r1 = [e for e in data["events"]
              if e["round"] == 1 and e["type"] == "card_revealed"]
        assert len(r1) == 1 and r1[0]["vtime_ms"] == 7000
        assert data["rounds"][0]["ended"] is True
        conn.close()

        # 继续（不重置）场景：恢复后 resume，新事件从冻结值累计
        with tempfile.TemporaryDirectory() as d2:
            path2 = os.path.join(d2, "t.db")
            rid2, gid2, cid2 = _make_running_drill_before_crash(path2)
            conn = fresh_conn(path2)
            dbmod.init_schema(conn)
            frozen2 = conn.execute("SELECT elapsed_ms FROM rooms WHERE id=?",
                                   (rid2,)).fetchone()["elapsed_ms"]
            recover2 = paused_events(conn, rid2)[0]["created_at"]
            rw = recover2 + 100
            conn.execute(
                "UPDATE rooms SET status='running', last_resume_at=? WHERE id=?", (rw, rid2))
            conn.commit()
            add_event(conn, rid2, rw, "clock",
                      {"action": "resumed", "status": "running", "elapsed_ms": frozen2})
            add_event(conn, rid2, rw + 5, "card_revealed",
                      {"id": cid2, "seq": 2, "title": "二次", "group_id": None,
                       "revealed_at_ms": frozen2 + 5000})
            high = conn.execute("SELECT MAX(id) m FROM events WHERE room_id=?",
                                (rid2,)).fetchone()["m"]
            room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid2,)).fetchone()
            data = rv.build_review_data(conn, room, high)
            last = [e for e in data["events"] if e["type"] == "card_revealed"][-1]
            assert last["vtime_ms"] == frozen2 + 5000
            conn.close()
    print("ok: resume accumulates from frozen value; reset keeps recovery event in old round")


if __name__ == "__main__":
    test_running_room_frozen_with_paused_event()
    test_reinit_idempotent_and_idle_paused_untouched()
    test_pause_after_restart_does_not_inflate_latency()
    test_resume_accumulates_from_frozen_and_reset_isolates_rounds()
    print("ALL RESTART-RECOVERY TESTS PASSED")
