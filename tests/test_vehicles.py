"""应急车辆申请服务端语义的无第三方依赖验证（python3 tests/test_vehicles.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在
app.db 建表的 SQLite 上调用 app.vehicles 域函数，复刻 main.py 路由在
state_lock 临界区内对车辆池/申请的操作次序，校验：

1. 车辆池：总量 1～20、每房间仅一个（重复创建 409），总量创建后固定；
2. 立即分配：无更早排队申请且余量足够才分配，余量从总量中正确扣减；
3. 入队规则：余量不足，或存在更早待分配申请（即使自己够分），一律排队；
4. 每组一份：未取消前重复提交 409，取消后可重新申请；
5. 数量校验：1～5 辆且不超过池总量，越界 400；
6. 补位：取消已分配申请释放车辆后按队首依次补位；队首数量不足时停止，
   绝不跳过队首去补后续申请（后续组即使够分也继续等待）；
7. 取消排队申请：不释放车辆、不产生补位，后续申请排位前移；
8. 重置：清空全部申请、余量恢复全部，车辆池保留，受理序号重新计数；
9. 级联：删除参与组/房间时车辆池与申请随之清理；
10. 重启持久化：关闭重连后车辆池、申请状态与排位一致；
11. 视野：主持人看总量/余量/各组申请与排位；参与组只看本组状态与排位；
12. SCHEMA：新表、索引与 UNIQUE(room_id, group_id) 真实存在。
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
from app import vehicles as vm  # noqa: E402


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(dbmod.SCHEMA)
    return conn


def make_room(conn, code="VEH001", name="车辆房"):
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
    except vm.VehicleError as exc:
        assert exc.status_code == status, (exc.status_code, exc.detail)
        return exc
    raise AssertionError(f"expected VehicleError {status}")


def test_pool_bounds_and_single_per_room():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        expect_error(400, vm.create_pool, conn, rid, 0)
        expect_error(400, vm.create_pool, conn, rid, 21)
        pool = vm.create_pool(conn, rid, 20)
        assert pool["total"] == 20
        assert vm.remaining(conn, rid) == 20
        # 每房间仅一个车辆池，重复创建 409（总量不可修改）
        expect_error(409, vm.create_pool, conn, rid, 1)
        # 边界值合法
        rid2 = make_room(conn, code="VEH002")
        p1 = vm.create_pool(conn, rid2, 1)
        assert p1["total"] == 1 and vm.remaining(conn, rid2) == 1
        # 未建池的房间余量为 None
        rid3 = make_room(conn, code="VEH003")
        assert vm.get_pool(conn, rid3) is None
        assert vm.remaining(conn, rid3) is None
        conn.close()
    print("ok: pool total must be 1~20, exactly one pool per room")


def test_immediate_allocation_while_sufficient():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR101")
        g2 = add_group(conn, rid, "蓝队", "BBB102")
        vm.create_pool(conn, rid, 5)

        a = vm.submit_request(conn, rid, g1, 2)
        assert a["status"] == "allocated" and a["queue_seq"] == 1
        assert vm.remaining(conn, rid) == 3
        b = vm.submit_request(conn, rid, g2, 3)
        assert b["status"] == "allocated" and b["queue_seq"] == 2
        assert vm.remaining(conn, rid) == 0
        conn.close()
    print("ok: first requests allocate immediately while remaining suffices")


def test_queue_when_insufficient_or_earlier_pending():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR201")
        g2 = add_group(conn, rid, "蓝队", "BBB202")
        g3 = add_group(conn, rid, "绿队", "GGG203")
        vm.create_pool(conn, rid, 5)

        # g1 分走 4 辆 → 余 1
        assert vm.submit_request(conn, rid, g1, 4)["status"] == "allocated"
        # g2 申请 2 辆：余量不足 → 入队（排位 1）
        q2 = vm.submit_request(conn, rid, g2, 2)
        assert q2["status"] == "queued"
        assert vm.my_view(conn, rid, g2)["position"] == 1
        # g3 申请 1 辆：虽然余量 1 足够，但存在更早待分配申请 → 仍排队（不越过队首）
        q3 = vm.submit_request(conn, rid, g3, 1)
        assert q3["status"] == "queued"
        assert vm.my_view(conn, rid, g3)["position"] == 2
        assert vm.remaining(conn, rid) == 1  # 余量未被 g3 占用
        view = vm.host_view(conn, rid)
        assert [r["group_id"] for r in view["requests"]] == [g1, g2, g3]
        assert [r["position"] for r in view["requests"]] == [None, 1, 2]
        conn.close()
    print("ok: queued on insufficient remaining or earlier pending request")


def test_one_active_request_and_reapply_after_cancel():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR301")
        vm.create_pool(conn, rid, 5)
        vm.submit_request(conn, rid, g1, 2)   # allocated，余 3
        expect_error(409, vm.submit_request, conn, rid, g1, 1)
        # 排队申请同样占用「每组一份」名额
        g2 = add_group(conn, rid, "蓝队", "BBB302")
        vm.submit_request(conn, rid, g2, 4)  # 余 3 < 4 → 排队
        expect_error(409, vm.submit_request, conn, rid, g2, 1)
        # 取消后可重新申请（g1 释放 2 辆后 g2 按队首补位 4 辆，余 1）
        cancelled, _ = vm.cancel_request(conn, rid, g1)
        assert cancelled["quantity"] == 2 and cancelled["status"] == "allocated"
        again = vm.submit_request(conn, rid, g1, 1)
        assert again["status"] == "allocated"  # 无更早排队且余量 1 足够 → 立即分配
        conn.close()
    print("ok: one active request per group; can reapply only after cancel")


def test_quantity_bounds_and_pool_cap():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR401")
        g2 = add_group(conn, rid, "蓝队", "BBB402")
        vm.create_pool(conn, rid, 3)
        expect_error(400, vm.submit_request, conn, rid, g1, 0)
        expect_error(400, vm.submit_request, conn, rid, g1, 6)
        # 不超过单次 5 辆上限，但超过池总量 3 辆 → 400
        expect_error(400, vm.submit_request, conn, rid, g1, 4)
        # 未建池房间提交 → 409
        rid2 = make_room(conn, code="VEH402")
        expect_error(409, vm.submit_request, conn, rid2, g2, 1)
        # 无申请时取消 → 404
        expect_error(404, vm.cancel_request, conn, rid, g2)
        conn.close()
    print("ok: quantity 1~5 and never above pool total")


def test_backfill_in_order_and_never_skip_head():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        gA = add_group(conn, rid, "红队", "RRR501")
        gB = add_group(conn, rid, "蓝队", "BBB502")
        gC = add_group(conn, rid, "绿队", "GGG503")
        gD = add_group(conn, rid, "白队", "WWW504")
        vm.create_pool(conn, rid, 5)

        # A 分 2、D 分 1 → 余 2；B 需 4（不足）排队第 1；C 需 1（有更早排队）排队第 2
        assert vm.submit_request(conn, rid, gA, 2)["status"] == "allocated"
        assert vm.submit_request(conn, rid, gD, 1)["status"] == "allocated"
        assert vm.submit_request(conn, rid, gB, 4)["status"] == "queued"
        assert vm.submit_request(conn, rid, gC, 1)["status"] == "queued"

        # D 取消（释放 1 辆，余 3）：队首 B 需 4 > 3 → 停止；
        # C 虽只需 1（3 足够）也绝不越过 B 补位
        _, newly = vm.cancel_request(conn, rid, gD)
        assert newly == []
        assert vm.my_view(conn, rid, gB)["status"] == "queued"
        assert vm.my_view(conn, rid, gC)["status"] == "queued"
        assert vm.remaining(conn, rid) == 3

        # A 取消（再释放 2 辆，余 5）：队首 B 需 4 ≤ 5 → 分配（余 1），
        # 继续看 C 需 1 ≤ 1 → 分配（余 0）
        _, newly2 = vm.cancel_request(conn, rid, gA)
        assert [r["group_id"] for r in newly2] == [gB, gC]  # 严格按队首顺序
        assert vm.my_view(conn, rid, gB)["status"] == "allocated"
        assert vm.my_view(conn, rid, gB)["position"] is None
        assert vm.my_view(conn, rid, gC)["status"] == "allocated"
        assert vm.remaining(conn, rid) == 0
        conn.close()
    print("ok: backfill walks queue head in order and never skips a short head")


def test_cancel_queued_shifts_positions_without_reallocating():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        gA = add_group(conn, rid, "红队", "RRR601")
        gB = add_group(conn, rid, "蓝队", "BBB602")
        gC = add_group(conn, rid, "绿队", "GGG603")
        vm.create_pool(conn, rid, 3)
        assert vm.submit_request(conn, rid, gA, 3)["status"] == "allocated"
        assert vm.submit_request(conn, rid, gB, 1)["status"] == "queued"
        assert vm.submit_request(conn, rid, gC, 1)["status"] == "queued"
        # 取消队首排队申请：不释放车辆，无补位；C 排位前移为 1
        cancelled, newly = vm.cancel_request(conn, rid, gB)
        assert cancelled["status"] == "queued" and newly == []
        assert vm.remaining(conn, rid) == 0
        assert vm.my_view(conn, rid, gC)["position"] == 1
        assert vm.my_view(conn, rid, gB) is None
        conn.close()
    print("ok: canceling queued request shifts positions, allocations unchanged")


def test_reset_clears_requests_but_keeps_pool():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR701")
        g2 = add_group(conn, rid, "蓝队", "BBB702")
        vm.create_pool(conn, rid, 4)
        vm.submit_request(conn, rid, g1, 3)           # allocated
        vm.submit_request(conn, rid, g2, 2)           # queued
        assert vm.remaining(conn, rid) == 1

        removed = vm.reset_requests(conn, rid)
        assert removed == 2
        # 车辆池保留，余量恢复全部
        pool = vm.get_pool(conn, rid)
        assert pool is not None and pool["total"] == 4
        assert vm.remaining(conn, rid) == 4
        assert vm.host_view(conn, rid)["requests"] == []
        # 新一轮可重新申请，受理序号从 1 重新计数
        row = vm.submit_request(conn, rid, g1, 4)
        assert row["status"] == "allocated" and row["queue_seq"] == 1
        conn.close()
    print("ok: reset clears all requests, restores remaining, keeps pool")


def test_cascade_delete_group_and_room():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR801")
        g2 = add_group(conn, rid, "蓝队", "BBB802")
        g3 = add_group(conn, rid, "绿队", "GGG803")
        vm.create_pool(conn, rid, 5)
        # g1 已分配 2，g2 排队 4（余 3 不足），g3 排队 1
        vm.submit_request(conn, rid, g1, 2)
        vm.submit_request(conn, rid, g2, 4)
        vm.submit_request(conn, rid, g3, 1)

        # 复刻 main.delete_group 在 state_lock 内的次序：删组 → 级联删申请
        # → fill_queue 补位（队首 g2 需 4 ≤ 释放后的余 5 → 分配，余 1；
        #   g3 需 1 ≤ 1 → 分配）
        conn.execute("DELETE FROM groups WHERE id = ?", (g1,))
        conn.commit()
        newly = vm.fill_queue(conn, rid)
        assert [r["group_id"] for r in newly] == [g2, g3]
        assert vm.remaining(conn, rid) == 0
        # g1 的申请已随组级联删除
        assert vm.request_of_group(conn, rid, g1) is None
        assert conn.execute(
            "SELECT COUNT(*) c FROM vehicle_requests WHERE group_id=?", (g1,)
        ).fetchone()["c"] == 0

        # 删除房间：车辆池与全部申请级联
        conn.execute("DELETE FROM rooms WHERE id = ?", (rid,))
        conn.commit()
        assert conn.execute("SELECT COUNT(*) c FROM vehicle_pools").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM vehicle_requests").fetchone()["c"] == 0
        conn.close()
    print("ok: cascade cleanup of pool/requests on group/room deletion")


def test_persistence_across_restart():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        rid = make_room(conn, code="VEH099")
        g1 = add_group(conn, rid, "红队", "RRR901")
        g2 = add_group(conn, rid, "蓝队", "BBB902")
        vm.create_pool(conn, rid, 3)
        vm.submit_request(conn, rid, g1, 2)   # allocated, remaining 1
        vm.submit_request(conn, rid, g2, 2)   # queued position 1
        conn.close()

        # 重启：重新连接并执行 init_schema（含迁移），状态全部由持久化行恢复
        conn2 = sqlite3.connect(path)
        conn2.row_factory = sqlite3.Row
        conn2.execute("PRAGMA foreign_keys=ON")
        dbmod.init_schema(conn2)
        pool = vm.get_pool(conn2, rid)
        assert pool is not None and pool["total"] == 3
        assert vm.remaining(conn2, rid) == 1
        v1 = vm.my_view(conn2, rid, g1)
        assert v1["status"] == "allocated" and v1["quantity"] == 2
        v2 = vm.my_view(conn2, rid, g2)
        assert v2["status"] == "queued" and v2["position"] == 1 and v2["quantity"] == 2
        view = vm.host_view(conn2, rid)
        assert view["remaining"] == 1 and len(view["requests"]) == 2
        conn2.close()
    print("ok: pool/requests/positions survive restart unchanged")


def test_schema_tables_indexes_and_unique():
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        names = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"vehicle_pools", "vehicle_requests"} <= names
        indexes = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
        assert "idx_vehicle_requests_room" in indexes
        # 每房间每组仅一份申请：UNIQUE(room_id, group_id)
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "RRR0A1")
        vm.create_pool(conn, rid, 5)
        vm.submit_request(conn, rid, g1, 1)
        try:
            conn.execute(
                "INSERT INTO vehicle_requests (room_id,group_id,quantity,status,queue_seq,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
                (rid, g1, 2, "queued", 99, time.time(), time.time()))
            conn.commit()
            raise AssertionError("UNIQUE(room_id, group_id) 未生效")
        except sqlite3.IntegrityError:
            pass
        # 车辆池每房间唯一：UNIQUE(room_id)
        try:
            conn.execute(
                "INSERT INTO vehicle_pools (room_id,total,created_at) VALUES (?,?,?)",
                (rid, 9, time.time()))
            conn.commit()
            raise AssertionError("UNIQUE(room_id) on vehicle_pools 未生效")
        except sqlite3.IntegrityError:
            pass
        conn.close()
    print("ok: schema creates vehicle tables/indexes and UNIQUE constraints")


if __name__ == "__main__":
    test_pool_bounds_and_single_per_room()
    test_immediate_allocation_while_sufficient()
    test_queue_when_insufficient_or_earlier_pending()
    test_one_active_request_and_reapply_after_cancel()
    test_quantity_bounds_and_pool_cap()
    test_backfill_in_order_and_never_skip_head()
    test_cancel_queued_shifts_positions_without_reallocating()
    test_reset_clears_requests_but_keeps_pool()
    test_cascade_delete_group_and_room()
    test_persistence_across_restart()
    test_schema_tables_indexes_and_unique()
    print("ALL VEHICLE TESTS PASSED")
