"""现场决策投票服务端语义的无第三方依赖验证（python3 tests/test_polls.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在
app.db 建表的 SQLite 上调用 app.polls 域函数，复刻 main.py 路由/tick 在
state_lock 临界区内对投票的操作次序，校验：

1. 创建/投票/改投：2～4 个选项，仅白名单组可投，UNIQUE(poll, group) 每组一票，
   改投原地更新（票数随之迁移）；
2. tick 一次性结算：到期结算、未到不结算，最高票获胜；平票按选项创建顺序；
   全员零票时选项 0 获胜；每条投票只结算一次（重复 tick / 重启后不重复）；
3. 暂停冻结截止计时：paused 房间的 tick 不结算（main 的 ticker 只扫 running，
   这里直接验证截止判定以虚拟毫秒为准，墙钟推进不影响）；
4. 重置语义：open 投票作废且选票删除，settled 结果按轮次保留；
5. 轮次隔离：只有当前轮次的 open 投票参与结算，旧轮次遗留行不动；
6. 级联：删除参与组/房间时投票关联表随之清理；
7. 旧库迁移：全新 SCHEMA 含全部投票表与索引。

本轮（结算固定票数）补充：
8. 结算当时固定 tallies/逐组 votes 到 poll_results；结算后删除投过票的参与组
   级联清掉残余选票，固定票数与获胜项不变（覆盖「获胜项残余 0 票、另一项 1 票」
   的旧矛盾场景）；重启换连接读到的仍是结算当时结果；
9. 旧库恢复：已结算行无 poll_results 时从对应 poll_settled 事件恢复票数；
   事件缺失只保留获胜项并标记 recoverable=0（空 tallies/votes），绝不拿残余
   选票重算；恢复幂等；删除房间级联清理 poll_results。
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
from app import polls as pm  # noqa: E402


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(dbmod.SCHEMA)
    return conn


def make_room(conn, code="POL001", status="running", elapsed_ms=0, now=None):
    now = now if now is not None else time.time()
    rid = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
        " VALUES (?,?,?,?,?,?)",
        (code, "投票房", status, elapsed_ms,
         now if status == "running" else None, now),
    ).lastrowid
    conn.commit()
    return rid, now


def add_group(conn, rid, name, code):
    gid = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, name, code, time.time()),
    ).lastrowid
    conn.commit()
    return gid


def make_poll(conn, rid, options, group_ids, deadline_ms, round_idx=0, question="决策"):
    return pm.create_poll(conn, rid, question, options, group_ids,
                          deadline_ms, round_idx)


def test_create_and_option_bounds():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn, status="idle")
        g1 = add_group(conn, rid, "红队", "RRR001")
        g2 = add_group(conn, rid, "蓝队", "BBB002")
        poll = make_poll(conn, rid, ["切流", "回滚"], [g1, g2], 30_000)
        assert poll["status"] == "open" and poll["round"] == 0
        opts = pm.poll_options(conn, poll["id"])
        assert [o["seq"] for o in opts] == [0, 1]
        assert [o["text"] for o in opts] == ["切流", "回滚"]
        assert pm.poll_group_ids(conn, poll["id"]) == [g1, g2]
        # 去重保序的白名单
        poll2 = make_poll(conn, rid, ["a", "b", "c", "d"], [g1, g1, g2], 60_000)
        assert pm.poll_group_ids(conn, poll2["id"]) == [g1, g2]
        assert len(pm.poll_options(conn, poll2["id"])) == 4
        conn.close()
    print("ok: poll create with 2~4 options and group whitelist")


def test_one_vote_per_group_and_change():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR011")
        g2 = add_group(conn, rid, "蓝队", "BBB012")
        poll = make_poll(conn, rid, ["A", "B"], [g1, g2], 60_000)
        oa, ob = pm.poll_options(conn, poll["id"])

        r1 = pm.cast_vote(conn, poll, g1, oa["id"])
        assert r1.first_vote is True
        # 同组再投 = 改投：原地更新，仍只有一条票行
        r2 = pm.cast_vote(conn, poll, g1, ob["id"])
        assert r2.first_vote is False and r2.option_id == ob["id"]
        rows = conn.execute(
            "SELECT * FROM poll_votes WHERE poll_id=? AND group_id=?",
            (poll["id"], g1)).fetchall()
        assert len(rows) == 1 and rows[0]["option_id"] == ob["id"]
        # 第二组独立投票
        pm.cast_vote(conn, poll, g2, oa["id"])
        n = conn.execute("SELECT COUNT(*) c FROM poll_votes WHERE poll_id=?",
                         (poll["id"],)).fetchone()["c"]
        assert n == 2
        conn.close()
    print("ok: one vote per group, change vote migrates in place")


def test_settle_winner_and_tie_by_option_order():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR021")
        g2 = add_group(conn, rid, "蓝队", "BBB022")
        g3 = add_group(conn, rid, "绿队", "GGG023")

        # B 得 2 票、A 得 1 票 → B 获胜
        p1 = make_poll(conn, rid, ["A", "B"], [g1, g2, g3], 10_000)
        oa, ob = pm.poll_options(conn, p1["id"])
        pm.cast_vote(conn, p1, g1, oa["id"])
        pm.cast_vote(conn, p1, g2, ob["id"])
        pm.cast_vote(conn, p1, g3, ob["id"])
        res = pm.settle_due_polls(conn, rid, 0, 10_000)
        assert len(res) == 1
        assert res[0].winner["id"] == ob["id"]
        assert res[0].tallies == {oa["id"]: 1, ob["id"]: 2}
        fresh = conn.execute("SELECT * FROM polls WHERE id=?", (p1["id"],)).fetchone()
        assert fresh["status"] == "settled"
        assert fresh["winner_option_id"] == ob["id"]
        assert fresh["settled_at_ms"] == 10_000

        # 平票：A、B 各 1 票 → 按创建顺序（seq 最小）A 获胜
        p2 = make_poll(conn, rid, ["A", "B"], [g1, g2], 20_000)
        a2, b2 = pm.poll_options(conn, p2["id"])
        pm.cast_vote(conn, p2, g1, a2["id"])
        pm.cast_vote(conn, p2, g2, b2["id"])
        res2 = pm.settle_due_polls(conn, rid, 0, 20_000)
        assert len(res2) == 1 and res2[0].winner["id"] == a2["id"]

        # 全员零票：所有选项同票（0），选项 0 获胜（确定性）
        p3 = make_poll(conn, rid, ["X", "Y", "Z"], [g1, g2], 30_000)
        x3 = pm.poll_options(conn, p3["id"])[0]
        res3 = pm.settle_due_polls(conn, rid, 0, 30_000)
        assert len(res3) == 1 and res3[0].winner["id"] == x3["id"]
        conn.close()
    print("ok: settle picks max votes, ties resolved by option creation order")


def test_settle_is_one_shot_and_not_before_deadline():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, t0 = make_room(conn, status="running")
        g1 = add_group(conn, rid, "红队", "RRR031")
        poll = make_poll(conn, rid, ["A", "B"], [g1], 60_000)
        oa, _ = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, oa["id"])
        # 未到截止点（虚拟毫秒 < deadline）：不结算
        assert pm.settle_due_polls(conn, rid, 0, 59_999) == []
        assert conn.execute("SELECT status FROM polls WHERE id=?",
                            (poll["id"],)).fetchone()["status"] == "open"
        # 到期结算一次
        res = pm.settle_due_polls(conn, rid, 0, 60_000)
        assert len(res) == 1
        # 模拟后续 tick / 重启恢复：同一投票绝不再结算（结果不变、不重复产出）
        assert pm.settle_due_polls(conn, rid, 0, 120_000) == []
        assert pm.settle_due_polls(conn, rid, 0, 999_999) == []
        n_settled = conn.execute(
            "SELECT COUNT(*) c FROM polls WHERE status='settled'").fetchone()["c"]
        assert n_settled == 1
        # 截止后投票被拒由 main 路由判定；域层仍可写入但结算口径只看 open，
        # 已结算行不接受二次结算（winner 不被改票影响）
        conn.close()
    print("ok: polls settle exactly once, never before virtual deadline")


def test_pause_freezes_deadline_clock():
    """暂停冻结：结算只由调用方在 running 时以虚拟毫秒触发。

    复刻 main.ticker_loop 的口径——它只 SELECT status='running' 的房间，
    paused 房间根本不会进入 settle_due_polls；这里显式验证：墙钟任意推进，
    只要调用方给出的虚拟毫秒（暂停时为冻结值）未到 deadline，就不结算。
    """
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        # paused at 30s frozen
        rid, t0 = make_room(conn, status="paused", elapsed_ms=30_000)
        g1 = add_group(conn, rid, "红队", "RRR041")
        poll = make_poll(conn, rid, ["A", "B"], [g1], 60_000)
        oa, _ = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, oa["id"])
        # 墙钟走多久都无所谓：以冻结虚拟毫秒 30s 结算 → 未到期
        assert pm.settle_due_polls(conn, rid, 0, 30_000) == []
        assert pm.settle_due_polls(conn, rid, 0, 30_000) == []
        assert conn.execute("SELECT status FROM polls WHERE id=?",
                            (poll["id"],)).fetchone()["status"] == "open"
        # 继续后虚拟时钟到 60s（调用方传入新的 elapsed）才结算
        res = pm.settle_due_polls(conn, rid, 0, 60_000)
        assert len(res) == 1 and res[0].winner["id"] == oa["id"]
        conn.close()
    print("ok: pause freezes deadline timing (virtual-ms driven)")


def test_reset_voids_open_but_keeps_settled():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR051")
        g2 = add_group(conn, rid, "蓝队", "BBB052")

        # 第一条：已结算（第一轮结果，须保留）
        p_settled = make_poll(conn, rid, ["A", "B"], [g1, g2], 10_000, round_idx=0)
        oa, ob = pm.poll_options(conn, p_settled["id"])
        pm.cast_vote(conn, p_settled, g1, oa["id"])
        pm.cast_vote(conn, p_settled, g2, oa["id"])
        settled = pm.settle_due_polls(conn, rid, 0, 10_000)
        assert len(settled) == 1

        # 第二条：open 投票（重置时作废，票删除）
        p_open = make_poll(conn, rid, ["C", "D"], [g1], 20_000, round_idx=0)
        oc, _ = pm.poll_options(conn, p_open["id"])
        pm.cast_vote(conn, p_open, g1, oc["id"])

        voided = pm.void_open_polls(conn, rid)
        assert voided == [p_open["id"]]
        row_open = conn.execute("SELECT * FROM polls WHERE id=?",
                                (p_open["id"],)).fetchone()
        assert row_open["status"] == "voided"
        # 选票随作废删除
        assert conn.execute(
            "SELECT COUNT(*) c FROM poll_votes WHERE poll_id=?",
            (p_open["id"],)).fetchone()["c"] == 0
        # 选项/白名单保留（作废投票元数据仍可复盘）
        assert len(pm.poll_options(conn, p_open["id"])) == 2
        assert pm.poll_group_ids(conn, p_open["id"]) == [g1]

        # 已结算结果原样保留（含获胜者与选票）
        row_s = conn.execute("SELECT * FROM polls WHERE id=?",
                             (p_settled["id"],)).fetchone()
        assert row_s["status"] == "settled"
        assert row_s["winner_option_id"] == oa["id"]
        assert conn.execute(
            "SELECT COUNT(*) c FROM poll_votes WHERE poll_id=?",
            (p_settled["id"],)).fetchone()["c"] == 2
        # 作废幂等：再次重置不影响已作废/已结算行
        assert pm.void_open_polls(conn, rid) == []
        conn.close()
    print("ok: reset voids open polls (votes deleted), keeps settled results")


def test_round_isolation_on_settle_and_new_round_votes():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR061")

        # 旧轮（round 0）遗留的 open 行：当前轮 1 的 tick 不得结算它
        old = make_poll(conn, rid, ["A", "B"], [g1], 10_000, round_idx=0)
        oa, _ = pm.poll_options(conn, old["id"])
        pm.cast_vote(conn, old, g1, oa["id"])
        assert pm.settle_due_polls(conn, rid, 1, 999_000) == []
        assert conn.execute("SELECT status FROM polls WHERE id=?",
                            (old["id"],)).fetchone()["status"] == "open"
        # 但旧轮自身视角仍可结算（同一行只结算一次）
        assert len(pm.settle_due_polls(conn, rid, 0, 10_000)) == 1

        # 新一轮（round 1）的投票独立统计；旧票行 round=0 不计入
        new = make_poll(conn, rid, ["C", "D"], [g1], 5_000, round_idx=1)
        oc, od = pm.poll_options(conn, new["id"])
        pm.cast_vote(conn, new, g1, od["id"])
        res = pm.settle_due_polls(conn, rid, 1, 5_000)
        assert len(res) == 1
        # 只数本轮（round=1）的票 → D 一票获胜；旧轮票绝不混入
        assert res[0].winner["id"] == od["id"]
        assert res[0].tallies == {oc["id"]: 0, od["id"]: 1}
        conn.close()
    print("ok: settlement scoped to current round; stale rows untouched")


def test_cascade_delete_group_and_room():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR071")
        g2 = add_group(conn, rid, "蓝队", "BBB072")
        poll = make_poll(conn, rid, ["A", "B"], [g1, g2], 60_000)
        oa, _ = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, oa["id"])
        # 删除参与组：白名单关联与该组选票级联清理（投票本身仍在）
        conn.execute("DELETE FROM groups WHERE id=?", (g1,))
        conn.commit()
        assert pm.poll_group_ids(conn, poll["id"]) == [g2]
        assert conn.execute(
            "SELECT COUNT(*) c FROM poll_votes WHERE poll_id=? AND group_id=?",
            (poll["id"], g1)).fetchone()["c"] == 0
        # 删除房间：投票/选项/白名单/选票全部级联
        conn.execute("DELETE FROM rooms WHERE id=?", (rid,))
        conn.commit()
        for table in ("polls", "poll_options", "poll_groups", "poll_votes"):
            assert conn.execute(f"SELECT COUNT(*) c FROM {table}").fetchone()["c"] == 0, table
        conn.close()
    print("ok: cascade cleanup on group/room deletion")


def test_restart_freezes_running_room_and_polls_survive():
    """重启恢复：init_schema 把 running 房间冻结为 paused（与卡片同一机制），
    期间不会因墙钟跳跃而结算；投票行全部持久化，恢复后可继续/再结算。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        t0 = time.time() - 90  # 假装 90 秒前进入 running
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('POL099','重启','running',0,?,?)", (t0, t0)).lastrowid
        conn.commit()
        g1 = add_group(conn, rid, "红队", "RRR091")
        poll = make_poll(conn, rid, ["A", "B"], [g1], 60_000)
        oa, _ = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, oa["id"])
        conn.close()

        # 重启：重新连接并执行 init_schema（含迁移），running → paused，
        # 已流逝墙钟 90s 被结算进冻结值；冻结后 ticker 不会扫到该房间，
        # 而冻结虚拟毫秒（90s）已过截止点——恢复后首次手动/继续到点只结算一次。
        conn2 = sqlite3.connect(path)
        conn2.row_factory = sqlite3.Row
        conn2.execute("PRAGMA foreign_keys=ON")
        dbmod.init_schema(conn2)
        room = conn2.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        assert room["status"] == "paused"
        assert room["elapsed_ms"] >= 90_000
        # 投票与选票仍在，状态持久（未被自动结算——结算只发生在 running tick 中）
        assert conn2.execute("SELECT status FROM polls WHERE id=?",
                             (poll["id"],)).fetchone()["status"] == "open"
        assert conn2.execute("SELECT COUNT(*) c FROM poll_votes WHERE poll_id=?",
                             (poll["id"],)).fetchone()["c"] == 1
        conn2.close()
    print("ok: restart freezes running room; polls persisted, no surprise settlement")


def test_legacy_schema_includes_poll_tables():
    """全新库 SCHEMA 直接建出投票四表与索引；_migrate 在全新库为空操作。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        names = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"polls", "poll_options", "poll_groups", "poll_votes"} <= names
        indexes = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
        assert {"idx_polls_room", "idx_poll_groups_group", "idx_poll_votes_poll"} <= indexes
        # UNIQUE 约束真实存在
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR081")
        poll = make_poll(conn, rid, ["A", "B"], [g1], 60_000)
        oa, ob = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, oa["id"])
        try:
            conn.execute(
                "INSERT INTO poll_votes (room_id,poll_id,group_id,option_id,round,updated_at)"
                " VALUES (?,?,?,?,?,?)",
                (rid, poll["id"], g1, ob["id"], 0, time.time()))
            conn.commit()
            raise AssertionError("UNIQUE(poll_id, group_id) 未生效")
        except sqlite3.IntegrityError:
            pass
        conn.close()
    print("ok: schema creates poll tables/indexes and UNIQUE(poll, group)")


def test_settled_tallies_fixed_survive_group_delete():
    """结算固定：A、B 各一票平票 → 按选项顺序 A 获胜；结算后删除投 A 的组，
    poll_votes 被级联成「A 零票、B 一票」的残余状态。固定快照必须仍是结算当时
    A=1、B=1 且获胜项 A（旧实现按残余票重算会显示获胜项 0 票、B 1 票的矛盾）。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        ga = add_group(conn, rid, "红队", "RRR101")
        gb = add_group(conn, rid, "蓝队", "BBB102")
        poll = make_poll(conn, rid, ["A", "B"], [ga, gb], 10_000)
        oa, ob = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, ga, oa["id"])  # 红队投 A（最终获胜项）
        pm.cast_vote(conn, poll, gb, ob["id"])  # 蓝队投 B
        res = pm.settle_due_polls(conn, rid, 0, 10_000)
        assert len(res) == 1 and res[0].winner["id"] == oa["id"]  # 平票按 seq → A
        assert res[0].tallies == {oa["id"]: 1, ob["id"]: 1}

        # 删除投了获胜项 A 的红队：选票随组级联删除（残余只剩 B 一票）
        conn.execute("DELETE FROM groups WHERE id=?", (ga,))
        conn.commit()
        residual = conn.execute(
            "SELECT option_id, COUNT(*) c FROM poll_votes WHERE poll_id=? GROUP BY option_id",
            (poll["id"],)).fetchall()
        assert [(r["option_id"], r["c"]) for r in residual] == [(ob["id"], 1)]

        # 固定快照不随残余票变化：A 仍 1 票（获胜项）、B 仍 1 票
        fixed = pm.fixed_result(conn, poll["id"])
        assert fixed is not None and fixed.recoverable is True
        fixed_tallies = {t["option_id"]: t["count"] for t in fixed.tallies}
        assert fixed_tallies == {oa["id"]: 1, ob["id"]: 1}
        assert {(v["group_id"], v["option_id"]) for v in fixed.votes} == {
            (ga, oa["id"]), (gb, ob["id"])}
        row = conn.execute("SELECT * FROM polls WHERE id=?", (poll["id"],)).fetchone()
        assert row["status"] == "settled" and row["winner_option_id"] == oa["id"]

        # 再结算任何投票都不影响已固定行；未结算投票仍按残余选票实时统计
        assert pm.settle_due_polls(conn, rid, 0, 999_999) == []
        conn.close()
    print("ok: settled tallies fixed at settlement; group delete cannot rewrite result")


def test_fixed_result_survives_restart_reconnect():
    """删组后重启（重新连接 + init_schema）：已结算结果仍读固定快照，与删组后、
    重启前完全一致；刷新（重新读视图，见 test_poll_views）同理。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR111")
        g2 = add_group(conn, rid, "蓝队", "BBB112")
        poll = make_poll(conn, rid, ["A", "B"], [g1, g2], 10_000)
        oa, ob = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, ob["id"])  # B 一票获胜
        pm.cast_vote(conn, poll, g2, ob["id"])
        pm.settle_due_polls(conn, rid, 0, 10_000)
        conn.execute("DELETE FROM groups WHERE id=?", (g1,))  # 删掉一张投 B 的票
        conn.commit()
        conn.close()

        conn2 = sqlite3.connect(path)
        conn2.row_factory = sqlite3.Row
        conn2.execute("PRAGMA foreign_keys=ON")
        dbmod.init_schema(conn2)  # 模拟重启：迁移/恢复（已固定行不应被改动）
        fixed = pm.fixed_result(conn2, poll["id"])
        fixed_tallies = {t["option_id"]: t["count"] for t in fixed.tallies}
        assert fixed.recoverable is True
        assert fixed_tallies == {oa["id"]: 0, ob["id"]: 2}
        assert len(fixed.votes) == 2
        row = conn2.execute("SELECT winner_option_id FROM polls WHERE id=?",
                            (poll["id"],)).fetchone()
        assert row["winner_option_id"] == ob["id"]
        conn2.close()
    print("ok: fixed settlement result survives restart after group deletion")


def _settled_row_legacy(conn, rid, options=("A", "B"), votes=()):
    """构造旧版本已结算投票：只写 polls/poll_options/poll_votes，不写 poll_results，
    模拟旧库升级瞬间的状态（winner 已固定、票数只散落在残余选票里）。"""
    gids = []
    poll = pm.create_poll(conn, rid, "旧投票", list(options), [], 10_000, 0)
    opts = pm.poll_options(conn, poll["id"])
    for i, (gname, gcode, opt_idx) in enumerate(votes):
        gid = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, gname, gcode, time.time())).lastrowid
        conn.execute(
            "INSERT INTO poll_groups (poll_id,group_id) VALUES (?,?)", (poll["id"], gid))
        conn.execute(
            "INSERT INTO poll_votes (room_id,poll_id,group_id,option_id,round,updated_at)"
            " VALUES (?,?,?,?,0,?)",
            (rid, poll["id"], gid, opts[opt_idx]["id"], time.time()))
        gids.append(gid)
    conn.execute(
        "UPDATE polls SET status='settled', winner_option_id=?, settled_at_ms=10000"
        " WHERE id=?", (opts[0]["id"], poll["id"]))  # 旧版固定的获胜项 = 选项 A
    conn.commit()
    return poll, opts, gids


def _add_poll_settled_event(conn, rid, poll, opts, tallies, votes):
    cur = conn.execute(
        "INSERT INTO events (room_id,type,payload,created_at) VALUES (?,?,?,?)",
        (rid, "poll_settled",
         json.dumps({"id": poll["id"], "status": "settled",
                     "tallies": [{"option_id": oid, "count": n} for oid, n in tallies],
                     "votes": [{"group_id": gid, "option_id": oid} for gid, oid in votes]},
                    ensure_ascii=False),
         time.time()))
    conn.commit()
    return cur.lastrowid


def test_legacy_recovers_tallies_from_settlement_event():
    """旧库已结算记录：升级时从对应 poll_settled 事件恢复固定票数——
    哪怕残余选票已被级联删成矛盾分布，恢复的仍是事件里的结算当时票数。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid, _ = make_room(conn)
        # 旧版结算当时：A、B 各一票，平票 A 获胜
        poll, opts, (ga, gb) = _settled_row_legacy(
            conn, rid, votes=(("红队", "RRR121", 0), ("蓝队", "BBB122", 1)))
        oa, ob = opts
        ev_id = _add_poll_settled_event(
            conn, rid, poll, opts,
            tallies=((oa["id"], 1), (ob["id"], 1)),
            votes=((ga, oa["id"]), (gb, ob["id"])))
        # 升级后红队被删：残余只剩 B 一票（旧矛盾现场）
        conn.execute("DELETE FROM groups WHERE id=?", (ga,))
        conn.commit()

        dbmod.init_schema(conn)  # 旧库启动：建 poll_results + 从事件恢复
        fixed = pm.fixed_result(conn, poll["id"])
        assert fixed is not None and fixed.recoverable is True
        assert fixed.recovered_from_event == ev_id
        fixed_tallies = {t["option_id"]: t["count"] for t in fixed.tallies}
        assert fixed_tallies == {oa["id"]: 1, ob["id"]: 1}
        assert len(fixed.votes) == 2  # 逐组选票也按事件恢复（含已删组的历史票）

        # 幂等：再次 init_schema 不重复插入、不改恢复来源
        dbmod.init_schema(conn)
        n = conn.execute("SELECT COUNT(*) c FROM poll_results WHERE poll_id=?",
                         (poll["id"],)).fetchone()["c"]
        assert n == 1
        conn.close()
    print("ok: legacy settled poll recovers tallies from poll_settled event")


def test_legacy_missing_event_keeps_winner_marks_unrecoverable():
    """结算事件缺失：不得用残余选票伪造票数——只保留获胜项，
    recoverable=0、tallies/votes 为空，由读取侧明确标示票数不可恢复。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid, _ = make_room(conn)
        # 残余选票：只剩 B 一票（A 的票随组被级联删除）；旧版固定的获胜项是 A
        poll, opts, (gb,) = _settled_row_legacy(
            conn, rid, votes=(("蓝队", "BBB131", 1),))
        oa, ob = opts

        dbmod.init_schema(conn)  # 无 poll_settled 事件可恢复
        fixed = pm.fixed_result(conn, poll["id"])
        assert fixed is not None and fixed.recoverable is False
        assert fixed.recovered_from_event is None
        assert fixed.tallies == [] and fixed.votes == []
        row = conn.execute("SELECT * FROM polls WHERE id=?", (poll["id"],)).fetchone()
        assert row["winner_option_id"] == oa["id"]  # 获胜项保留
        assert row["status"] == "settled"
        # 残余选票原样未动（读取侧不得拿它重算）
        residual = conn.execute(
            "SELECT option_id FROM poll_votes WHERE poll_id=?", (poll["id"],)).fetchall()
        assert [r["option_id"] for r in residual] == [ob["id"]]
        conn.close()
    print("ok: missing settlement event keeps winner, marks tallies unrecoverable")


def test_poll_results_cascade_with_room():
    """删除房间：固定快照随投票级联清理。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid, _ = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR141")
        poll = make_poll(conn, rid, ["A", "B"], [g1], 10_000)
        oa, _ = pm.poll_options(conn, poll["id"])
        pm.cast_vote(conn, poll, g1, oa["id"])
        pm.settle_due_polls(conn, rid, 0, 10_000)
        assert conn.execute("SELECT COUNT(*) c FROM poll_results").fetchone()["c"] == 1
        conn.execute("DELETE FROM rooms WHERE id=?", (rid,))
        conn.commit()
        assert conn.execute("SELECT COUNT(*) c FROM poll_results").fetchone()["c"] == 0
        conn.close()
    print("ok: poll_results cascade with room deletion")


def test_legacy_duplicate_settlement_events_picks_one():
    """同一投票若存在多条 poll_settled 事件（异常/手工重复写入），恢复确定性
    取最早一条且只写一行 poll_results，不因 JOIN 扇出而撞主键。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid, _ = make_room(conn)
        poll, opts, (ga, gb) = _settled_row_legacy(
            conn, rid, votes=(("红队", "RRR151", 0), ("蓝队", "BBB152", 1)))
        oa, ob = opts
        payload_tallies = ((oa["id"], 1), (ob["id"], 1))
        payload_votes = ((ga, oa["id"]), (gb, ob["id"]))
        ev1 = _add_poll_settled_event(conn, rid, poll, opts,
                                      payload_tallies, payload_votes)
        _add_poll_settled_event(conn, rid, poll, opts,
                                payload_tallies, payload_votes)  # 重复事件
        dbmod.init_schema(conn)
        rows = conn.execute("SELECT * FROM poll_results WHERE poll_id=?",
                            (poll["id"],)).fetchall()
        assert len(rows) == 1 and rows[0]["recoverable"] == 1
        assert rows[0]["recovered_from_event"] == ev1  # 取最早一条
        conn.close()
    print("ok: duplicate settlement events recover one row from earliest event")


if __name__ == "__main__":
    test_create_and_option_bounds()
    test_one_vote_per_group_and_change()
    test_settle_winner_and_tie_by_option_order()
    test_settle_is_one_shot_and_not_before_deadline()
    test_pause_freezes_deadline_clock()
    test_reset_voids_open_but_keeps_settled()
    test_round_isolation_on_settle_and_new_round_votes()
    test_cascade_delete_group_and_room()
    test_restart_freezes_running_room_and_polls_survive()
    test_legacy_schema_includes_poll_tables()
    test_settled_tallies_fixed_survive_group_delete()
    test_fixed_result_survives_restart_reconnect()
    test_legacy_recovers_tallies_from_settlement_event()
    test_legacy_missing_event_keeps_winner_marks_unrecoverable()
    test_legacy_duplicate_settlement_events_picks_one()
    test_poll_results_cascade_with_room()
    print("ALL POLL TESTS PASSED")
