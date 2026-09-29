"""应急车辆申请 —— 车辆池与申请队列域（不依赖 fastapi，便于无第三方依赖测试）。

与 app.main 同一把 state_lock、同一个 SQLite 连接下被调用，因此本模块
不再自行加锁，只负责：

- 车辆池：每房间一个，总量固定 1～20 辆，创建后不可改；重置演练不清池
- 申请：每组同时仅一份（UNIQUE(room_id, group_id)），数量 1～5 且不超过池总量；
  取消后才能重新申请
- 受理顺序分配：调用方在 state_lock 内串行调用，queue_seq 单调递增即受理顺序；
  无更早待分配申请且余量足够 → 立即分配，否则入队
- 补位：已分配申请被取消（或所属组被删除）释放车辆后，从队首依次补位；
  队首数量不足时停止，绝不跳过队首去补后续申请
- 重置：清空全部申请、余量恢复全量，车辆池保留

状态全部由持久化行推导（vehicle_pools / vehicle_requests 两表），
刷新、断线重连与服务重启后经快照恢复，不丢不乱。
"""

from __future__ import annotations

import sqlite3
import time

MIN_POOL_TOTAL = 1
MAX_POOL_TOTAL = 20
MIN_QUANTITY = 1
MAX_QUANTITY = 5


class VehicleError(Exception):
    """车辆申请域校验错误。main.py 捕获后转成对应 HTTP 状态码（detail 为消息文本）。"""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# ---------------------------------------------------------------- 车辆池

def create_pool(conn: sqlite3.Connection, room_id: int, total: int,
                *, now: float | None = None) -> sqlite3.Row:
    """创建车辆池：总量 1～20 辆、创建后固定；每房间仅一个，重复创建返回 409。"""
    if not (MIN_POOL_TOTAL <= total <= MAX_POOL_TOTAL):
        raise VehicleError(400, f"车辆池总量须为 {MIN_POOL_TOTAL}～{MAX_POOL_TOTAL} 辆")
    if get_pool(conn, room_id) is not None:
        raise VehicleError(409, "车辆池已创建，总量固定不可修改")
    now = time.time() if now is None else now
    conn.execute(
        "INSERT INTO vehicle_pools (room_id, total, created_at) VALUES (?, ?, ?)",
        (room_id, total, now),
    )
    conn.commit()
    return get_pool(conn, room_id)


def get_pool(conn: sqlite3.Connection, room_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM vehicle_pools WHERE room_id = ?", (room_id,)
    ).fetchone()


def allocated_sum(conn: sqlite3.Connection, room_id: int) -> int:
    return conn.execute(
        "SELECT COALESCE(SUM(quantity), 0) AS s FROM vehicle_requests"
        " WHERE room_id = ? AND status = 'allocated'",
        (room_id,),
    ).fetchone()["s"]


def remaining(conn: sqlite3.Connection, room_id: int) -> int | None:
    """剩余可分配车辆；尚未创建车辆池时返回 None。"""
    pool = get_pool(conn, room_id)
    if pool is None:
        return None
    return pool["total"] - allocated_sum(conn, room_id)


# ---------------------------------------------------------------- 申请 / 受理

def request_of_group(conn: sqlite3.Connection, room_id: int,
                     group_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE room_id = ? AND group_id = ?",
        (room_id, group_id),
    ).fetchone()


def queued_requests(conn: sqlite3.Connection, room_id: int) -> list[sqlite3.Row]:
    """待分配队列，按受理顺序（queue_seq 递增）。"""
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE room_id = ? AND status = 'queued'"
        " ORDER BY queue_seq",
        (room_id,),
    ).fetchall()


def submit_request(conn: sqlite3.Connection, room_id: int, group_id: int, quantity: int,
                   *, now: float | None = None) -> sqlite3.Row:
    """参与组提交一份申请。调用方须先解析组身份（join_code → group）。

    受理规则（在 state_lock 内按受理顺序串行执行）：
    无更早待分配申请且余量足够 → 立即分配；否则入队等待补位。
    """
    pool = get_pool(conn, room_id)
    if pool is None:
        raise VehicleError(409, "主持人尚未创建车辆池")
    if not (MIN_QUANTITY <= quantity <= MAX_QUANTITY):
        raise VehicleError(400, f"申请数量须为 {MIN_QUANTITY}～{MAX_QUANTITY} 辆")
    if quantity > pool["total"]:
        raise VehicleError(400, "申请数量不能超过车辆池总量")
    if request_of_group(conn, room_id, group_id) is not None:
        raise VehicleError(409, "本组已有申请，取消后才能重新申请")
    now = time.time() if now is None else now
    seq = conn.execute(
        "SELECT COALESCE(MAX(queue_seq), 0) + 1 AS s FROM vehicle_requests WHERE room_id = ?",
        (room_id,),
    ).fetchone()["s"]
    has_earlier_queued = conn.execute(
        "SELECT 1 FROM vehicle_requests WHERE room_id = ? AND status = 'queued' LIMIT 1",
        (room_id,),
    ).fetchone() is not None
    rem = pool["total"] - allocated_sum(conn, room_id)
    status = "allocated" if (not has_earlier_queued and rem >= quantity) else "queued"
    cur = conn.execute(
        "INSERT INTO vehicle_requests (room_id, group_id, quantity, status, queue_seq,"
        " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (room_id, group_id, quantity, status, seq, now, now),
    )
    conn.commit()
    return conn.execute(
        "SELECT * FROM vehicle_requests WHERE id = ?", (cur.lastrowid,)
    ).fetchone()


def fill_queue(conn: sqlite3.Connection, room_id: int) -> list[sqlite3.Row]:
    """从队首依次补位：余量足够才分配，队首数量不足即停止（不跳过队首）。

    返回本次新分配的申请行（按补位顺序）。幂等：无可补位时为空操作。
    """
    pool = get_pool(conn, room_id)
    if pool is None:
        return []
    rem = pool["total"] - allocated_sum(conn, room_id)
    now = time.time()
    newly_ids: list[int] = []
    for row in queued_requests(conn, room_id):
        if row["quantity"] > rem:
            break  # 队首数量不足：不得跳过它去补后续申请
        conn.execute(
            "UPDATE vehicle_requests SET status = 'allocated', updated_at = ? WHERE id = ?",
            (now, row["id"]),
        )
        rem -= row["quantity"]
        newly_ids.append(row["id"])
    if newly_ids:
        conn.commit()
    return [conn.execute(
        "SELECT * FROM vehicle_requests WHERE id = ?", (i,)
    ).fetchone() for i in newly_ids]


def cancel_request(conn: sqlite3.Connection, room_id: int,
                   group_id: int) -> tuple[sqlite3.Row, list[sqlite3.Row]]:
    """取消本组申请（之后可重新申请）。返回 (被取消的申请行, 补位新分配的行列表)。

    取消已分配申请会释放车辆：随即从队首依次补位；取消排队申请不释放车辆，
    无需补位（后续申请排位自然前移，由调用方广播同步）。
    """
    row = request_of_group(conn, room_id, group_id)
    if row is None:
        raise VehicleError(404, "本组没有可取消的申请")
    conn.execute("DELETE FROM vehicle_requests WHERE id = ?", (row["id"],))
    conn.commit()
    newly = fill_queue(conn, room_id) if row["status"] == "allocated" else []
    return row, newly


def reset_requests(conn: sqlite3.Connection, room_id: int) -> int:
    """重置演练：清空全部申请（余量随之恢复全量），车辆池保留。返回清除条数。"""
    cur = conn.execute("DELETE FROM vehicle_requests WHERE room_id = ?", (room_id,))
    conn.commit()
    return cur.rowcount


# ---------------------------------------------------------------- 视野

def _positions(conn: sqlite3.Connection, room_id: int) -> dict[int, int]:
    """排队申请的排位：按受理顺序（queue_seq）从 1 起编号。"""
    return {row["id"]: i + 1 for i, row in enumerate(queued_requests(conn, room_id))}


def host_view(conn: sqlite3.Connection, room_id: int) -> dict | None:
    """主持人视野：车辆池总量/余量 + 各组申请与队列（按受理顺序）。未建池返回 None。"""
    pool = get_pool(conn, room_id)
    if pool is None:
        return None
    positions = _positions(conn, room_id)
    names = {r["id"]: r["name"] for r in conn.execute(
        "SELECT id, name FROM groups WHERE room_id = ?", (room_id,))}
    requests = []
    for row in conn.execute(
        "SELECT * FROM vehicle_requests WHERE room_id = ? ORDER BY queue_seq", (room_id,)
    ):
        requests.append({
            "id": row["id"],
            "group_id": row["group_id"],
            "group_name": names.get(row["group_id"], f"#{row['group_id']}"),
            "quantity": row["quantity"],
            "status": row["status"],              # allocated | queued
            "position": positions.get(row["id"]),  # 仅排队中有排位（1 起）
            "created_at": row["created_at"],
        })
    return {
        "total": pool["total"],
        "remaining": pool["total"] - allocated_sum(conn, room_id),
        "requests": requests,
    }


def my_view(conn: sqlite3.Connection, room_id: int, group_id: int) -> dict | None:
    """参与组视野：仅本组申请状态与排位（看不到他组申请与池余量）；无申请返回 None。"""
    row = request_of_group(conn, room_id, group_id)
    if row is None:
        return None
    return {
        "id": row["id"],
        "quantity": row["quantity"],
        "status": row["status"],
        "position": _positions(conn, room_id).get(row["id"]),  # 仅排队中有排位
        "created_at": row["created_at"],
    }
