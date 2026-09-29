"""现场情报核验板服务端语义的无第三方依赖验证（python3 tests/test_intel.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在
app.db 建表的 SQLite 上调用 app.intel 域函数，复刻 main.py 路由在
state_lock 临界区内对命题/报告的操作次序，校验：

1. 创建命题：至少两个不同参与组（去重保序），白名单落库；
2. 提交/修改报告：立场须为 support/oppose，依据必填且 ≤500 字；
   同组修改覆盖原报告（原地更新、revision +1），UNIQUE(claim, group)
   保证每组每命题仅一份最新报告——同组多次提交绝不计为多份证据；
3. 状态即时重算：两边均有报告 → 有争议；同一立场 ≥2 组且对立为零 →
   已证实/已否定；其余 → 待核验；单边仅一组时仍是待核验；
4. 撤回：报告删除后状态即时回落；无报告可撤时返回 None（路由转 404）；
5. 重置：清空全部报告（各命题回到待核验），命题与白名单保留；
6. 级联：删除参与组时其报告与白名单行随之清理（状态由剩余组报告重算），
   删除房间时三表全部清理；
7. 重启持久化：关闭重连后命题、报告与状态一致；
8. SCHEMA：新表、索引与 UNIQUE(claim_id, group_id) 真实存在。
"""
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import intel as im  # noqa: E402


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(dbmod.SCHEMA)
    return conn


def make_room(conn, code="INT001", name="核验房"):
    rid = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,created_at) VALUES (?,?,?,?,?)",
        (code, name, "idle", 0, time.time()),
    ).lastrowid
    conn.commit()
    return rid


def add_group(conn, rid, name, code):
    gid = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, name, code, time.time()),
    ).lastrowid
    conn.commit()
    return gid


def expect_error(status, fn, *args):
    try:
        fn(*args)
    except im.IntelError as exc:
        assert exc.status_code == status, (exc.status_code, exc.detail)
        return exc
    raise AssertionError(f"expected IntelError {status}")


def make_claim(conn, rid, group_ids, statement="东区变电站已恢复供电"):
    return im.create_claim(conn, rid, statement, group_ids, 0)


def test_create_requires_at_least_two_groups():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR001")
        g2 = add_group(conn, rid, "蓝队", "BBB002")
        g3 = add_group(conn, rid, "绿队", "GGG003")
        # 少于两个参与组 → 400
        expect_error(400, im.create_claim, conn, rid, "命题", [], 0)
        expect_error(400, im.create_claim, conn, rid, "命题", [g1], 0)
        expect_error(400, im.create_claim, conn, rid, "命题", [g1, g1], 0)
        # 两个及以上合法；重复组去重保序
        claim = make_claim(conn, rid, [g2, g1, g2, g3])
        assert claim["statement"] == "东区变电站已恢复供电"
        assert claim["round"] == 0
        assert im.claim_group_ids(conn, claim["id"]) == [g1, g2, g3]
        assert im.group_in_whitelist(conn, claim["id"], g1)
        assert not im.group_in_whitelist(conn, claim["id"], 99999)
        # 新命题必然零报告、待核验
        assert im.summary(conn, claim["id"]) == {
            "support": 0, "oppose": 0, "status": "pending"}
        conn.close()
    print("ok: claim requires >=2 distinct groups, whitelist persisted")


def test_submit_validation_and_overwrite_in_place():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR011")
        g2 = add_group(conn, rid, "蓝队", "BBB012")
        claim = make_claim(conn, rid, [g1, g2])

        # 立场与依据校验
        expect_error(400, im.submit_report, conn, claim, g1, "neutral", "依据")
        expect_error(400, im.submit_report, conn, claim, g1, "support", "   ")
        expect_error(400, im.submit_report, conn, claim, g1, "support", "x" * 501)

        r1 = im.submit_report(conn, claim, g1, "support", "现场目测已复电")
        assert r1["stance"] == "support" and r1["revision"] == 1
        # 同组修改：覆盖原报告（同一行原地更新、revision +1），仍只有一行
        r2 = im.submit_report(conn, claim, g1, "oppose", "复核：仍未复电")
        assert r2["id"] == r1["id"]
        assert r2["stance"] == "oppose" and r2["rationale"] == "复核：仍未复电"
        assert r2["revision"] == 2
        rows = conn.execute(
            "SELECT * FROM intel_reports WHERE claim_id=? AND group_id=?",
            (claim["id"], g1)).fetchall()
        assert len(rows) == 1
        # 同组多次提交绝不计为多份证据：反对计数仍是 1
        assert im.stance_counts(conn, claim["id"]) == {"support": 0, "oppose": 1}
        conn.close()
    print("ok: submit validates stance/rationale; same-group update overwrites in place")


def test_status_recomputed_from_latest_reports_only():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR021")
        g2 = add_group(conn, rid, "蓝队", "BBB022")
        g3 = add_group(conn, rid, "绿队", "GGG023")
        g4 = add_group(conn, rid, "白队", "WWW024")
        claim = make_claim(conn, rid, [g1, g2, g3, g4])
        cid = claim["id"]

        # 单边仅一组 → 仍待核验
        im.submit_report(conn, claim, g1, "support", "依据一")
        assert im.summary(conn, cid)["status"] == "pending"
        # 同一立场两组且对立为零 → 已证实
        im.submit_report(conn, claim, g2, "support", "依据二")
        s = im.summary(conn, cid)
        assert s == {"support": 2, "oppose": 0, "status": "confirmed"}
        # 同组反复提交不增加证据数（g2 改依据三次仍是一组）
        im.submit_report(conn, claim, g2, "support", "依据二改")
        im.submit_report(conn, claim, g2, "support", "依据二再改")
        assert im.summary(conn, cid)["support"] == 2
        # 对立立场出现 → 有争议（哪怕 2:1）
        im.submit_report(conn, claim, g3, "oppose", "反证一")
        s = im.summary(conn, cid)
        assert s == {"support": 2, "oppose": 1, "status": "disputed"}
        # 同组改立场：计数即时迁移（g1 支持→反对 → 1:2 仍有争议）
        im.submit_report(conn, claim, g1, "oppose", "改判")
        s = im.summary(conn, cid)
        assert s == {"support": 1, "oppose": 2, "status": "disputed"}
        # 撤回最后一组支持 → 反对两组且支持为零 → 已否定
        im.withdraw_report(conn, cid, g2)
        s = im.summary(conn, cid)
        assert s == {"support": 0, "oppose": 2, "status": "refuted"}
        # 再撤一组 → 单边仅一组 → 回到待核验
        im.withdraw_report(conn, cid, g1)
        s = im.summary(conn, cid)
        assert s == {"support": 0, "oppose": 1, "status": "pending"}
        conn.close()
    print("ok: status recomputed from distinct groups' latest reports")


def test_withdraw_semantics():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR031")
        g2 = add_group(conn, rid, "蓝队", "BBB032")
        claim = make_claim(conn, rid, [g1, g2])
        # 无报告可撤 → None（main 路由转 404）
        assert im.withdraw_report(conn, claim["id"], g1) is None
        im.submit_report(conn, claim, g1, "support", "依据")
        withdrawn = im.withdraw_report(conn, claim["id"], g1)
        assert withdrawn["group_id"] == g1 and withdrawn["stance"] == "support"
        assert im.report_of_group(conn, claim["id"], g1) is None
        # 撤回后可重新提交（revision 重新从 1 计）
        again = im.submit_report(conn, claim, g1, "oppose", "新依据")
        assert again["revision"] == 1 and again["stance"] == "oppose"
        conn.close()
    print("ok: withdraw removes report; resubmission starts fresh")


def test_reset_clears_reports_but_keeps_claims():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR041")
        g2 = add_group(conn, rid, "蓝队", "BBB042")
        c1 = make_claim(conn, rid, [g1, g2], "命题一")
        c2 = make_claim(conn, rid, [g1, g2], "命题二")
        im.submit_report(conn, c1, g1, "support", "依据")
        im.submit_report(conn, c1, g2, "support", "依据")
        im.submit_report(conn, c2, g1, "oppose", "依据")
        assert im.summary(conn, c1["id"])["status"] == "confirmed"

        removed = im.reset_reports(conn, rid)
        assert removed == 3
        # 命题与白名单保留，状态回到待核验
        assert im.get_claim(conn, rid, c1["id"])["statement"] == "命题一"
        assert im.claim_group_ids(conn, c1["id"]) == [g1, g2]
        assert im.claim_group_ids(conn, c2["id"]) == [g1, g2]
        assert im.summary(conn, c1["id"]) == {
            "support": 0, "oppose": 0, "status": "pending"}
        assert conn.execute(
            "SELECT COUNT(*) c FROM intel_reports WHERE room_id=?", (rid,)
        ).fetchone()["c"] == 0
        # 新一轮可重新提交
        r = im.submit_report(conn, c1, g1, "support", "新一轮依据")
        assert r["revision"] == 1
        conn.close()
    print("ok: reset clears reports, keeps claims and whitelists")


def test_cascade_delete_group_and_room():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR051")
        g2 = add_group(conn, rid, "蓝队", "BBB052")
        g3 = add_group(conn, rid, "绿队", "GGG053")
        claim = make_claim(conn, rid, [g1, g2, g3])
        cid = claim["id"]
        im.submit_report(conn, claim, g1, "support", "依据一")
        im.submit_report(conn, claim, g2, "support", "依据二")
        im.submit_report(conn, claim, g3, "oppose", "依据三")
        assert im.summary(conn, cid)["status"] == "disputed"

        # 删除参与组：其报告与白名单行级联清理，状态由剩余组报告重算
        # （复刻 main.delete_group 的次序：先取受影响命题，删组后重算）
        affected = [r["claim_id"] for r in conn.execute(
            "SELECT claim_id FROM intel_claim_groups WHERE group_id=?", (g3,))]
        assert affected == [cid]
        conn.execute("DELETE FROM groups WHERE id=?", (g3,))
        conn.commit()
        assert im.claim_group_ids(conn, cid) == [g1, g2]
        assert im.report_of_group(conn, cid, g3) is None
        assert im.summary(conn, cid) == {
            "support": 2, "oppose": 0, "status": "confirmed"}

        # 删除房间：命题/白名单/报告全部级联
        conn.execute("DELETE FROM rooms WHERE id=?", (rid,))
        conn.commit()
        for table in ("intel_claims", "intel_claim_groups", "intel_reports"):
            assert conn.execute(
                f"SELECT COUNT(*) c FROM {table}").fetchone()["c"] == 0, table
        conn.close()
    print("ok: cascade cleanup on group/room deletion; status recomputed")


def test_persistence_across_restart():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid = make_room(conn, code="INT099")
        g1 = add_group(conn, rid, "红队", "RRR091")
        g2 = add_group(conn, rid, "蓝队", "BBB092")
        claim = make_claim(conn, rid, [g1, g2])
        im.submit_report(conn, claim, g1, "support", "依据一")
        im.submit_report(conn, claim, g2, "support", "依据二")
        conn.close()

        # 重启：重新连接并执行 init_schema（含迁移），状态全部由持久化行恢复
        conn2 = sqlite3.connect(path)
        conn2.row_factory = sqlite3.Row
        conn2.execute("PRAGMA foreign_keys=ON")
        dbmod.init_schema(conn2)
        c = im.get_claim(conn2, rid, claim["id"])
        assert c is not None and c["statement"] == "东区变电站已恢复供电"
        assert im.claim_group_ids(conn2, c["id"]) == [g1, g2]
        reports = im.reports_of_claim(conn2, c["id"])
        assert [r["group_id"] for r in reports] == [g1, g2]
        assert im.summary(conn2, c["id"]) == {
            "support": 2, "oppose": 0, "status": "confirmed"}
        conn2.close()
    print("ok: claims/reports/status survive restart unchanged")


def test_schema_tables_indexes_and_unique():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        names = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"intel_claims", "intel_claim_groups", "intel_reports"} <= names
        indexes = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
        assert {"idx_intel_claims_room", "idx_intel_claim_groups_group",
                "idx_intel_reports_claim", "idx_intel_reports_room"} <= indexes
        # 每组每命题仅一份报告：UNIQUE(claim_id, group_id)
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR0A1")
        g2 = add_group(conn, rid, "蓝队", "BBB0A2")
        claim = make_claim(conn, rid, [g1, g2])
        im.submit_report(conn, claim, g1, "support", "依据")
        try:
            conn.execute(
                "INSERT INTO intel_reports (room_id,claim_id,group_id,stance,rationale,"
                "revision,updated_at) VALUES (?,?,?,?,?,?,?)",
                (rid, claim["id"], g1, "oppose", "重复", 1, time.time()))
            conn.commit()
            raise AssertionError("UNIQUE(claim_id, group_id) 未生效")
        except sqlite3.IntegrityError:
            pass
        # 白名单主键 (claim_id, group_id)
        try:
            conn.execute(
                "INSERT INTO intel_claim_groups (claim_id, group_id) VALUES (?, ?)",
                (claim["id"], g1))
            conn.commit()
            raise AssertionError("PRIMARY KEY(claim_id, group_id) 未生效")
        except sqlite3.IntegrityError:
            pass
        conn.close()
    print("ok: schema creates intel tables/indexes and UNIQUE constraints")


if __name__ == "__main__":
    test_create_requires_at_least_two_groups()
    test_submit_validation_and_overwrite_in_place()
    test_status_recomputed_from_latest_reports_only()
    test_withdraw_semantics()
    test_reset_clears_reports_but_keeps_claims()
    test_cascade_delete_group_and_room()
    test_persistence_across_restart()
    test_schema_tables_indexes_and_unique()
    print("ALL INTEL TESTS PASSED")
