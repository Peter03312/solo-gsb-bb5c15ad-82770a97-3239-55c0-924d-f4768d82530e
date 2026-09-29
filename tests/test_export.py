"""export 模块的无第三方依赖验证脚本（python3 tests/test_export.py）。

直接用 app.db 的建表语句在临时 SQLite 上构造两轮完整演练，
校验：水位边界、脱敏、空演练、答复版本轨迹、时钟动作、时间线
虚拟时间与同刻事件按 ID 排序、ZIP 与离线回放页可生成。
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
        "INSERT INTO events (room_id, type, payload, created_at) VALUES (?,?,?,?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), t),
    )
    conn.commit()
    return cur.lastrowid


def make_room(conn, name, status="paused", elapsed_ms=0, last_resume=None):
    cur = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
        " VALUES (?,?,?,?,?,?)",
        ("ABC234", name, status, elapsed_ms, last_resume, time.time()),
    )
    conn.commit()
    return cur.lastrowid


def test_empty_drill():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn, "空演练")
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        data = rv.build_review_data(conn, room, 0)
        assert data["high_water_event_id"] == 0
        assert data["orchestration"]["groups"] == []
        assert data["orchestration"]["cards"] == []
        assert data["events"] == []
        assert data["clock_actions"] == []
        assert data["rounds"] == [{"round": 0, "ended": False, "end_ms": 0}]
        blob = rv.build_review_zip(data)
        with zipfile.ZipFile(__import__("io").BytesIO(blob)) as zf:
            names = set(zf.namelist())
            assert {"replay.html", "data.json", "README.txt"} <= names
            html = zf.read("replay.html").decode("utf-8")
            assert "__REPLAY_DATA_JSON__" not in html
            assert "空演练" in html
            again = json.loads(zf.read("data.json").decode("utf-8"))
            assert again["high_water_event_id"] == 0
        conn.close()
    print("ok: empty drill export opens and contains replay page")


def test_full_drill():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "核心库故障")

        # 编排：两个组（带 join_code）、两张全员卡 + 一张门槛卡
        g1 = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, "红队", "AAA111", t0)).lastrowid
        g2 = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, "蓝队", "BBB222", t0)).lastrowid
        c1 = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,content,group_id)"
            " VALUES (?,?,?,?,?,?)", (rid, 1, 10, "告警", "CPU 高", None)).lastrowid
        c2 = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,content,group_id)"
            " VALUES (?,?,?,?,?,?)", (rid, 2, 20, "定向给红队", "排查", g1)).lastrowid
        c3 = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,content,group_id,"
            "depends_on_card_id,gate_timeout_seconds) VALUES (?,?,?,?,?,?,?,?)",
            (rid, 3, 30, "门槛卡", "等前置", None, c1, 60)).lastrowid
        conn.commit()

        e_group1 = add_event(conn, rid, t0 + 1, "group_added",
                             {"id": g1, "name": "红队", "join_code": "AAA111"})
        e_group2 = add_event(conn, rid, t0 + 2, "group_added",
                             {"id": g2, "name": "蓝队", "join_code": "BBB222"})
        e_card1 = add_event(conn, rid, t0 + 3, "card_added",
                            {"id": c1, "seq": 1, "trigger_seconds": 10, "title": "告警",
                             "content": "CPU 高", "group_id": None})
        e_card2 = add_event(conn, rid, t0 + 4, "card_added",
                            {"id": c2, "seq": 2, "trigger_seconds": 20, "title": "定向给红队",
                             "content": "排查", "group_id": g1})
        e_card3 = add_event(conn, rid, t0 + 5, "card_added",
                            {"id": c3, "seq": 3, "trigger_seconds": 30, "title": "门槛卡",
                             "content": "等前置", "group_id": None})

        # 观察分享（含 token）——必须被剔除
        e_share = add_event(conn, rid, t0 + 6, "share_created",
                            {"token": "SECRET-TOKEN-XYZ", "group_ids": [g1]})

        # 开始（t0+10 墙钟 → 虚拟 0 起步）
        wall_start = t0 + 10
        e_start = add_event(conn, rid, wall_start, "clock",
                            {"action": "started", "status": "running", "elapsed_ms": 0})
        conn.execute("UPDATE rooms SET status='running', elapsed_ms=0, last_resume_at=? WHERE id=?",
                     (wall_start, rid))
        conn.commit()

        # 10s 揭示 c1
        conn.execute("UPDATE cards SET revealed=1, revealed_at_ms=? WHERE id=?", (10000, c1))
        conn.commit()
        e_rev1 = add_event(conn, rid, wall_start + 10.0, "card_revealed",
                           {"id": c1, "seq": 1, "trigger_seconds": 10, "title": "告警",
                            "group_id": None, "revealed": True, "revealed_at_ms": 10000,
                            "timed_out": False, "answers_locked": False})
        # 同刻（同一墙钟秒）红队回执、蓝队回执 —— 同刻按事件 ID 排序
        conn.execute("INSERT INTO receipts (room_id,card_id,group_id,created_at) VALUES (?,?,?,?)",
                     (rid, c1, g1, wall_start + 10.0))
        conn.execute("INSERT INTO receipts (room_id,card_id,group_id,created_at) VALUES (?,?,?,?)",
                     (rid, c1, g2, wall_start + 10.0))
        conn.commit()
        e_ack1 = add_event(conn, rid, wall_start + 10.0, "receipt_updated",
                           {"card_id": c1, "group_id": g1, "acked": True})
        e_ack2 = add_event(conn, rid, wall_start + 10.0, "receipt_updated",
                           {"card_id": c1, "group_id": g2, "acked": True})

        # 红队答复两个版本（v1 at 11s, v2 at 12s）
        conn.execute(
            "INSERT INTO answers (room_id,card_id,group_id,body,revision,updated_at)"
            " VALUES (?,?,?,?,1,?)", (rid, c1, g1, "初判：重启", wall_start + 11))
        conn.commit()
        e_a1 = add_event(conn, rid, wall_start + 11.0, "answer_updated",
                         {"card_id": c1, "group_id": g1, "body": "初判：重启", "revision": 1,
                          "updated_at": wall_start + 11})
        conn.execute("UPDATE answers SET body=?, revision=2, updated_at=? WHERE card_id=? AND group_id=?",
                     ("再判：扩容", wall_start + 12, c1, g1))
        conn.commit()
        e_a2 = add_event(conn, rid, wall_start + 12.0, "answer_updated",
                         {"card_id": c1, "group_id": g1, "body": "再判：扩容", "revision": 2,
                          "updated_at": wall_start + 12})

        # 15s 暂停
        conn.execute("UPDATE rooms SET status='paused', elapsed_ms=15000, last_resume_at=NULL WHERE id=?",
                     (rid,))
        conn.commit()
        e_pause = add_event(conn, rid, wall_start + 15.0, "clock",
                            {"action": "paused", "status": "paused", "elapsed_ms": 15000})

        # 20s 继续（墙钟 t0+40）：run_start 15s
        wall_resume = t0 + 40
        conn.execute("UPDATE rooms SET status='running', last_resume_at=? WHERE id=?",
                     (wall_resume, rid))
        conn.commit()
        e_resume = add_event(conn, rid, wall_resume, "clock",
                             {"action": "resumed", "status": "running", "elapsed_ms": 15000})

        # 虚拟 20s（墙钟 resume+5）揭示 c2
        conn.execute("UPDATE cards SET revealed=1, revealed_at_ms=? WHERE id=?", (20000, c2))
        conn.commit()
        e_rev2 = add_event(conn, rid, wall_resume + 5.0, "card_revealed",
                           {"id": c2, "seq": 2, "trigger_seconds": 20, "title": "定向给红队",
                            "group_id": g1, "revealed": True, "revealed_at_ms": 20000,
                            "timed_out": False, "answers_locked": False})

        # 虚拟 ~31s（resume+16）重置：c3 门槛已由 c1 回执解锁，在 30s 揭示
        conn.execute("UPDATE cards SET revealed=1, revealed_at_ms=? WHERE id=?", (30000, c3))
        conn.commit()
        e_rev3 = add_event(conn, rid, wall_resume + 15.0, "card_revealed",
                           {"id": c3, "seq": 3, "trigger_seconds": 30, "title": "门槛卡",
                            "group_id": None, "revealed": True, "revealed_at_ms": 30000,
                            "timed_out": False, "answers_locked": False})
        wall_reset = wall_resume + 16.0
        # 重置落库：清揭示/回执/答复（模拟 reset 路由效果）
        conn.execute("UPDATE cards SET revealed=0, revealed_at_ms=NULL, timed_out=0, answers_locked=0 WHERE room_id=?", (rid,))
        conn.execute("DELETE FROM receipts WHERE room_id=?", (rid,))
        conn.execute("DELETE FROM answers WHERE room_id=?", (rid,))
        conn.execute("UPDATE rooms SET status='idle', elapsed_ms=0, last_resume_at=NULL WHERE id=?", (rid,))
        conn.commit()
        e_reset = add_event(conn, rid, wall_reset, "reset",
                            {"action": "reset", "status": "idle", "elapsed_ms": 0})

        # 第二轮：开始 + c1 在 10s 再次揭示 + 红队新回执/新答复 v1
        wall2 = wall_reset + 100.0
        conn.execute("UPDATE rooms SET status='running', last_resume_at=? WHERE id=?", (wall2, rid))
        conn.commit()
        e_start2 = add_event(conn, rid, wall2, "clock",
                             {"action": "started", "status": "running", "elapsed_ms": 0})
        conn.execute("UPDATE cards SET revealed=1, revealed_at_ms=? WHERE id=?", (10000, c1))
        conn.execute("INSERT INTO receipts (room_id,card_id,group_id,created_at) VALUES (?,?,?,?)",
                     (rid, c1, g1, wall2 + 10))
        conn.execute("INSERT INTO answers (room_id,card_id,group_id,body,revision,updated_at)"
                     " VALUES (?,?,?,?,1,?)", (rid, c1, g1, "第二轮答复", wall2 + 12))
        conn.commit()
        e_rev1b = add_event(conn, rid, wall2 + 10.0, "card_revealed",
                            {"id": c1, "seq": 1, "trigger_seconds": 10, "title": "告警",
                             "group_id": None, "revealed": True, "revealed_at_ms": 10000,
                             "timed_out": False, "answers_locked": False})
        e_ack1b = add_event(conn, rid, wall2 + 10.0, "receipt_updated",
                            {"card_id": c1, "group_id": g1, "acked": True})
        e_a1b = add_event(conn, rid, wall2 + 12.0, "answer_updated",
                          {"card_id": c1, "group_id": g1, "body": "第二轮答复", "revision": 1,
                           "updated_at": wall2 + 12})

        high_water = conn.execute("SELECT MAX(id) m FROM events WHERE room_id=?", (rid,)).fetchone()["m"]
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        data = rv.build_review_data(conn, room, high_water)

        # 水位字段（时间线按 (轮次, 虚拟时间, ID) 排序，非全局 ID 序；下面单独校验排序）
        assert data["high_water_event_id"] == high_water
        ids = [e["id"] for e in data["events"]]
        assert max(ids) == high_water and all(i <= high_water for i in ids)

        # 脱敏：任何事件 / 字符串都不含凭证
        raw = json.dumps(data, ensure_ascii=False)
        for secret in ("AAA111", "BBB222", "ABC234", "SECRET-TOKEN-XYZ", "join_code", "token"):
            assert secret not in raw, f"secret leaked: {secret}"
        assert all(not e["type"].startswith("share_") for e in data["events"])

        # 编排
        orch = data["orchestration"]
        assert {g["name"] for g in orch["groups"]} == {"红队", "蓝队"}
        assert all("join_code" not in g for g in orch["groups"])
        seqs = [c["seq"] for c in orch["cards"]]
        assert seqs == [1, 2, 3]
        gate_card = orch["cards"][2]
        assert gate_card["gate"]["depends_on_seq"] == 1
        assert gate_card["gate"]["gate_timeout_seconds"] == 60

        # 水位时刻（第二轮运行中）：当前状态表 —— c1 已揭示，其余未揭示
        assert {c["id"] for c in data["revealed_cards"]} == {c1}
        receipts = data["receipts"]
        assert {(r["card_id"], r["group_id"]) for r in receipts} == {(c1, g1)}
        answers = data["answers"]
        assert len(answers) == 1 and answers[0]["body"] == "第二轮答复" and answers[0]["revision"] == 1

        # 答复版本轨迹：红队 c1 共 3 个版本（v1/v2 第一轮 + v1 第二轮），按事件 ID 升序
        vers = [v for v in data["answer_versions"] if v["card_id"] == c1 and v["group_id"] == g1]
        assert [v["revision"] for v in vers] == [1, 2, 1]
        assert [v["body"] for v in vers] == ["初判：重启", "再判：扩容", "第二轮答复"]

        # 时钟动作：start/pause/resume/reset/start，轮次标注正确
        acts = [(a["action"], a["round"], a["vtime_ms"]) for a in data["clock_actions"]]
        assert acts == [
            ("started", 0, 0), ("paused", 0, 15000), ("resumed", 0, 15000),
            ("reset", 0, 31000), ("started", 1, 0),
        ], acts

        # 轮次：第 0 轮 end=31000 已结束，第 1 轮 end=运行到水位的外推值
        rounds = data["rounds"]
        assert rounds[0]["ended"] is True and rounds[0]["end_ms"] == 31000
        assert rounds[1]["ended"] is False
        assert rounds[1]["end_ms"] >= 12000  # wall2+12 起算

        # 时间线虚拟时间：同墙钟秒的 c1 揭示/两回执 vtime 都是 10000，按 ID 排序
        tl = data["events"]
        at10 = [e for e in tl if e["round"] == 0 and e["vtime_ms"] == 10000]
        assert [e["type"] for e in at10] == ["card_revealed", "receipt_updated", "receipt_updated"]
        assert [e["id"] for e in at10] == sorted(e["id"] for e in at10)
        # resume 段：c2 揭示虚拟 20000（墙钟 resume+5），c3 揭示 30000
        vtime = {e["id"]: e["vtime_ms"] for e in tl}
        assert vtime[e_rev2] == 20000, vtime[e_rev2]
        assert vtime[e_rev3] == 30000, vtime[e_rev3]
        assert vtime[e_a2] == 12000  # 第一段运行中：wall_start+12
        # 暂停期间事件挂靠冻结值 15000（此处无暂停期间业务事件，仅校验 reset 前边界）
        assert vtime[e_reset] == 31000
        # 第二轮事件 round=1，vtime 重新从 0 起算
        assert vtime[e_rev1b] == 10000
        r1 = [e for e in tl if e["round"] == 1]
        assert r1 and all(e["vtime_ms"] >= 0 for e in r1)

        # 时间线整体按 (round, vtime, id) 排序
        keys = [(e["round"], e["vtime_ms"], e["id"]) for e in tl]
        assert keys == sorted(keys)

        # ZIP + 回放页
        blob = rv.build_review_zip(data)
        with zipfile.ZipFile(__import__("io").BytesIO(blob)) as zf:
            html = zf.read("replay.html").decode("utf-8")
            assert "__REPLAY_DATA_JSON__" not in html
            assert "__REVIEW_DATA_SUMMARY__" not in html
            assert "核心库故障" in html
            assert "AAA111" not in html and "SECRET-TOKEN-XYZ" not in html
            # 数据确实内联
            assert '"format": "inject-review"' in html or '"format":"inject-review"' in html

        # 文件名安全化
        fn = rv.review_filename("核心库 故障/演练:1", 1700000000)
        assert fn.endswith(".zip") and "/" not in fn and ":" not in fn

        conn.close()
    print("ok: full two-round drill: watermark, sanitization, versions, timeline, zip")


def test_watermark_boundary():
    """水位截断：id > high_water 的事件/状态不得出现（生成期间到达不混入）。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn, "w")
        g1 = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, "G", "ZZZ999", 1.0)).lastrowid
        c1 = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id,revealed,revealed_at_ms)"
            " VALUES (?,?,?,?,?,1,5000)", (rid, 1, 5, "old", None)).lastrowid
        conn.commit()
        e1 = add_event(conn, rid, 1.0, "group_added", {"id": g1, "name": "G", "join_code": "ZZZ999"})
        e2 = add_event(conn, rid, 2.0, "card_added", {"id": c1, "seq": 1, "trigger_seconds": 5,
                                                       "title": "old", "group_id": None})
        e3 = add_event(conn, rid, 3.0, "card_revealed", {"id": c1, "seq": 1, "title": "old",
                                                          "group_id": None})
        # 水位取 e2：揭示事件 e3 与任何后续都不得进入
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        data = rv.build_review_data(conn, room, e2)
        ids = [e["id"] for e in data["events"]]
        assert ids == [e1, e2]
        assert data["high_water_event_id"] == e2
        # 时间线中不存在揭示事件
        assert all(e["type"] != "card_revealed" for e in data["events"])
        conn.close()
    print("ok: watermark boundary excludes later events")


def add_anchor(conn, room_id, title, round_idx, vtime_ms, event_id, note="", created=100.0):
    """插入锚点行：event_id = 绑定水位；created_event_id 默认 = 绑定水位 + 1
    （与服务端锁内「先读水位、再落自身创建事件」的真实关系一致）。"""
    cur = conn.execute(
        "INSERT INTO anchors (room_id,title,note,round,vtime_ms,event_id,created_event_id,"
        "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (room_id, title, note, round_idx, vtime_ms, event_id, event_id + 1, created, created))
    conn.commit()
    return cur.lastrowid


def test_anchors_export():
    """复盘锚点：仅导出 event_id <= 水位者；按 (轮次, 虚拟毫秒, id) 排序；
    锚点事件不进脱敏事件流；跨重置归属原轮次；备注修改随最新值导出。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn, "锚点演练")

        e_start = add_event(conn, rid, 10.0, "clock",
                            {"action": "started", "status": "running", "elapsed_ms": 0})
        e_rev = add_event(conn, rid, 20.0, "card_revealed",
                          {"id": 1, "seq": 1, "title": "卡", "group_id": None})
        # 锚点 A：记录于 e_rev 之后（绑定水位 = e_rev，自身事件 = e_rev+1），虚拟 10s
        a1 = add_anchor(conn, rid, "切流前", 0, 10000, e_rev, note="先观察", created=21.0)
        e_a1_created = add_event(conn, rid, 21.0, "anchor_created",
                                 {"id": a1, "round": 0, "vtime_ms": 10000, "event_id": e_rev})
        assert e_a1_created == e_rev + 1  # 锁内绑定语义：自身事件恰为绑定水位 + 1
        # 修改备注：事件与行更新都在，但导出的锚点备注须为最新
        conn.execute("UPDATE anchors SET note=?, updated_at=? WHERE id=?",
                     ("先观察，再确认回滚方案", 22.0, a1))
        conn.commit()
        e_a1_updated = add_event(conn, rid, 22.0, "anchor_updated", {"id": a1})
        # 重置 → 第二轮
        e_reset = add_event(conn, rid, 30.0, "reset",
                            {"action": "reset", "status": "idle", "elapsed_ms": 0})
        # 第二轮锚点 B：绑定水位 = e_reset，自身事件 = e_reset + 1
        a2 = add_anchor(conn, rid, "第二轮起点", 1, 0, e_reset, created=40.0)
        e_a2_created = add_event(conn, rid, 40.0, "anchor_created",
                                 {"id": a2, "round": 1, "vtime_ms": 0, "event_id": e_reset})
        # 锚点 C：在 B 的创建事件之后记录（自身事件 = e_a2_created + 1）——
        # 水位若取 e_a2_created，C 尚未创建（created_event_id 更大），必须被排除
        a3 = add_anchor(conn, rid, "晚于水位", 1, 5000, e_a2_created, created=41.0)
        e_a3_created = add_event(conn, rid, 41.0, "anchor_created",
                                 {"id": a3, "round": 1, "vtime_ms": 5000, "event_id": e_a2_created})

        # 场景 1：水位 e_a1_updated（第二轮仅有 reset，锚点 B/C 均未创建）
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        data = rv.build_review_data(conn, room, e_a1_updated)
        anchors = data["anchors"]
        assert [a["id"] for a in anchors] == [a1]
        assert anchors[0]["note"] == "先观察，再确认回滚方案"
        assert anchors[0]["round"] == 0 and anchors[0]["vtime_ms"] == 10000
        assert anchors[0]["event_id"] == e_rev
        assert anchors[0]["created_event_id"] == e_a1_created
        # 锚点事件整类不进时间线
        assert all(not e["type"].startswith("anchor_") for e in data["events"])
        assert data["format_version"] >= 2

        # 场景 2：水位 e_a2_created（B 已创建；C 的 created_event_id = e_a2_created+1，排除）
        data2 = rv.build_review_data(conn, room, e_a2_created)
        ids2 = [(a["id"], a["round"], a["vtime_ms"]) for a in data2["anchors"]]
        assert ids2 == [(a1, 0, 10000), (a2, 1, 0)], ids2
        assert all(a["id"] != a3 for a in data2["anchors"])
        # B 的回放截断水位是 e_reset（点击 B 只重放到重置动作，不含任何第二轮新事件）
        b2 = next(a for a in data2["anchors"] if a["id"] == a2)
        assert b2["event_id"] == e_reset
        # 轮次元数据：重置发生在第二轮
        assert data2["rounds"][0]["ended"] is True

        # 场景 3：完整水位（MAX id）：三个锚点全在（C 的 event_id = e_a2_created <= max）
        high = conn.execute("SELECT MAX(id) m FROM events WHERE room_id=?", (rid,)).fetchone()["m"]
        data3 = rv.build_review_data(conn, room, high)
        assert {a["id"] for a in data3["anchors"]} == {a1, a2, a3}
        # 排序 (round, vtime_ms, id)
        keys = [(a["round"], a["vtime_ms"], a["id"]) for a in data3["anchors"]]
        assert keys == sorted(keys)

        # ZIP 内回放页含锚点数据与标题
        blob = rv.build_review_zip(data3)
        with zipfile.ZipFile(__import__("io").BytesIO(blob)) as zf:
            html = zf.read("replay.html").decode("utf-8")
            assert "切流前" in html and "第二轮起点" in html and "晚于水位" in html
            again = json.loads(zf.read("data.json").decode("utf-8"))
            assert {a["id"] for a in again["anchors"]} == {a1, a2, a3}
        conn.close()
    print("ok: anchors export watermark, ordering, rounds, note edit, zip")


def test_legacy_db_migration_adds_anchors():
    """旧库迁移：只有旧表、缺 anchors 表的库启动迁移后 anchors 可用；
    旧卡片缺列补齐；既有演练视为没有任何锚点。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "old.db")
        legacy = sqlite3.connect(path)
        legacy.row_factory = sqlite3.Row
        # 模拟最早期的旧库：rooms/groups/cards/events（cards 无任何新列）
        legacy.executescript("""
        CREATE TABLE rooms (
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'idle',
            elapsed_ms INTEGER NOT NULL DEFAULT 0, last_resume_at REAL, created_at REAL NOT NULL);
        CREATE TABLE groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT, room_id INTEGER NOT NULL,
            name TEXT NOT NULL, join_code TEXT NOT NULL UNIQUE, created_at REAL NOT NULL);
        CREATE TABLE cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT, room_id INTEGER NOT NULL,
            seq INTEGER NOT NULL, trigger_seconds INTEGER NOT NULL,
            title TEXT NOT NULL, content TEXT NOT NULL DEFAULT '');
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, room_id INTEGER NOT NULL,
            type TEXT NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL);
        """)
        legacy.execute(
            "INSERT INTO rooms (code,name,status,created_at) VALUES ('OLD001','旧', 'paused', 1.0)")
        legacy.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title) VALUES (1,1,5,'老卡片')")
        legacy.commit()
        legacy.close()

        # 走与启动完全一致的初始化路径
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        dbmod.init_schema(conn)

        cols = {r["name"] for r in conn.execute("PRAGMA table_info(cards)")}
        for needed in ("group_id", "depends_on_card_id", "gate_timeout_seconds",
                       "timed_out", "answers_locked"):
            assert needed in cols, f"legacy cards missing column {needed}"
        # anchors 表由 SCHEMA 的 IF NOT EXISTS 补齐
        names = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"anchors", "answers", "observer_shares", "receipts"} <= names
        n = conn.execute("SELECT COUNT(*) c FROM anchors").fetchone()["c"]
        assert n == 0
        # 旧库可以直接写入锚点并导出
        now = time.time()
        conn.execute(
            "INSERT INTO anchors (room_id,title,note,round,vtime_ms,event_id,created_event_id,"
            "created_at,updated_at) VALUES (1,'升级后锚点','',0,12000,0,0,?,?)", (now, now))
        conn.commit()
        room = conn.execute("SELECT * FROM rooms WHERE id=1").fetchone()
        data = rv.build_review_data(conn, room, 0)
        assert [a["title"] for a in data["anchors"]] == ["升级后锚点"]
        # 停机时 running 房间冻结逻辑不影响 paused 旧房间
        assert room["status"] == "paused"
        conn.close()
    print("ok: legacy db migration adds anchors table and missing card columns")


if __name__ == "__main__":
    test_empty_drill()
    test_full_drill()
    test_watermark_boundary()
    test_anchors_export()
    test_legacy_db_migration_adds_anchors()
    print("ALL EXPORT TESTS PASSED")
