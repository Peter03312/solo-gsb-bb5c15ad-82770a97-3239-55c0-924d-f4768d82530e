"""现场情报核验板 —— 待核验命题与组报告域（不依赖 fastapi，便于无第三方依赖测试）。

与 app.main 同一把 state_lock、同一个 SQLite 连接下被调用，因此本模块
不再自行加锁，只负责：

- 命题：主持人创建待核验命题并指定至少两个参与组（白名单）；
  命题跨重置保留（重置只清报告）
- 报告：白名单内参与组提交「支持 / 反对 + 简短依据」；
  UNIQUE(claim_id, group_id) 保证每组每命题仅一份最新报告——
  同组修改即原地覆盖（revision +1），绝不把同组多次提交计为多份证据；
  也可整份撤回
- 状态：服务端只按不同参与组的最新报告即时重算（报告行即唯一事实来源，
  不落冗余状态列）：
  · 两边立场均有报告          → 有争议 disputed
  · 同一立场 ≥2 组且对立为零  → 已证实 confirmed（支持）/ 已否定 refuted（反对）
  · 其余                      → 待核验 pending
  每次修改 / 撤回 / 白名单收缩（参与组被删）后由调用方重算并广播
- 重置：清空全部报告（命题与白名单保留），各命题回到待核验

状态全部由持久化行推导（intel_claims / intel_claim_groups / intel_reports
三表），刷新、断线重连与服务重启后经快照恢复，不丢不乱。
"""

from __future__ import annotations

import sqlite3
import time

MIN_CLAIM_GROUPS = 2          # 每条命题至少指定两个参与组
MAX_RATIONALE_LEN = 500       # 简短依据长度上限
STANCES = ("support", "oppose")

# 核验状态取值
STATUS_PENDING = "pending"        # 待核验
STATUS_CONFIRMED = "confirmed"    # 已证实
STATUS_REFUTED = "refuted"        # 已否定
STATUS_DISPUTED = "disputed"      # 有争议


class IntelError(Exception):
    """情报核验域校验错误。main.py 捕获后转成对应 HTTP 状态码（detail 为消息文本）。"""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# ---------------------------------------------------------------- 命题

def create_claim(
    conn: sqlite3.Connection,
    room_id: int,
    statement: str,
    group_ids: list[int],
    round_idx: int,
    *,
    now: float | None = None,
) -> sqlite3.Row:
    """创建一条待核验命题并写入参与组白名单（至少两个不同参与组）。
    调用方须先完成输入校验（命题非空、各组属于本房间）。"""
    group_ids = list(dict.fromkeys(group_ids))  # 去重保序
    if len(group_ids) < MIN_CLAIM_GROUPS:
        raise IntelError(400, f"每条命题至少指定 {MIN_CLAIM_GROUPS} 个参与组")
    now = time.time() if now is None else now
    cur = conn.execute(
        "INSERT INTO intel_claims (room_id, statement, round, created_at)"
        " VALUES (?, ?, ?, ?)",
        (room_id, statement, round_idx, now),
    )
    claim_id = cur.lastrowid
    for gid in group_ids:
        conn.execute(
            "INSERT INTO intel_claim_groups (claim_id, group_id) VALUES (?, ?)",
            (claim_id, gid),
        )
    conn.commit()
    return get_claim(conn, room_id, claim_id)


def get_claim(conn: sqlite3.Connection, room_id: int, claim_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM intel_claims WHERE id = ? AND room_id = ?", (claim_id, room_id)
    ).fetchone()


def claim_group_ids(conn: sqlite3.Connection, claim_id: int) -> list[int]:
    return [r["group_id"] for r in conn.execute(
        "SELECT group_id FROM intel_claim_groups WHERE claim_id = ? ORDER BY group_id",
        (claim_id,),
    )]


def group_in_whitelist(conn: sqlite3.Connection, claim_id: int, group_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM intel_claim_groups WHERE claim_id = ? AND group_id = ?",
        (claim_id, group_id),
    ).fetchone() is not None


# ---------------------------------------------------------------- 报告（提交 / 修改 / 撤回）

def report_of_group(
    conn: sqlite3.Connection, claim_id: int, group_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM intel_reports WHERE claim_id = ? AND group_id = ?",
        (claim_id, group_id),
    ).fetchone()


def reports_of_claim(conn: sqlite3.Connection, claim_id: int) -> list[sqlite3.Row]:
    """一条命题的全部最新报告（每组至多一行，按组排序）。"""
    return conn.execute(
        "SELECT * FROM intel_reports WHERE claim_id = ? ORDER BY group_id", (claim_id,)
    ).fetchall()


def submit_report(
    conn: sqlite3.Connection,
    claim: sqlite3.Row,
    group_id: int,
    stance: str,
    rationale: str,
    *,
    now: float | None = None,
) -> sqlite3.Row:
    """白名单参与组提交或修改本组报告：同组修改覆盖原报告（原地更新，revision +1）。

    调用方须先解析组身份并验证白名单；立场与依据的域校验在此完成。
    每组每命题至多一行（UNIQUE(claim_id, group_id)），同组多次提交
    只会覆盖最新报告，绝不会被计为多份证据。
    """
    if stance not in STANCES:
        raise IntelError(400, "立场须为 support（支持）或 oppose（反对）")
    rationale = rationale.strip()
    if not rationale:
        raise IntelError(400, "请填写简短依据")
    if len(rationale) > MAX_RATIONALE_LEN:
        raise IntelError(400, f"依据长度不能超过 {MAX_RATIONALE_LEN} 字")
    now = time.time() if now is None else now
    existing = report_of_group(conn, claim["id"], group_id)
    if existing is None:
        conn.execute(
            "INSERT INTO intel_reports (room_id, claim_id, group_id, stance, rationale,"
            " revision, updated_at) VALUES (?, ?, ?, ?, ?, 1, ?)",
            (claim["room_id"], claim["id"], group_id, stance, rationale, now),
        )
    else:
        conn.execute(
            "UPDATE intel_reports SET stance = ?, rationale = ?,"
            " revision = revision + 1, updated_at = ? WHERE id = ?",
            (stance, rationale, now, existing["id"]),
        )
    conn.commit()
    return report_of_group(conn, claim["id"], group_id)


def withdraw_report(
    conn: sqlite3.Connection, claim_id: int, group_id: int
) -> sqlite3.Row | None:
    """撤回本组报告。返回被撤回的行；本组没有报告时返回 None（调用方转 404）。"""
    row = report_of_group(conn, claim_id, group_id)
    if row is None:
        return None
    conn.execute("DELETE FROM intel_reports WHERE id = ?", (row["id"],))
    conn.commit()
    return row


# ---------------------------------------------------------------- 状态计算

def stance_counts(conn: sqlite3.Connection, claim_id: int) -> dict:
    """按不同参与组的最新报告统计两边立场组数（每组只计一次）。"""
    counts = {"support": 0, "oppose": 0}
    for row in conn.execute(
        "SELECT stance, COUNT(*) AS c FROM intel_reports WHERE claim_id = ?"
        " GROUP BY stance",
        (claim_id,),
    ):
        if row["stance"] in counts:  # 防御：非法立场不计入
            counts[row["stance"]] = row["c"]
    return counts


def status_of(support: int, oppose: int) -> str:
    """核验状态：两边均有报告 → 有争议；同一立场 ≥2 组且对立为零 → 已证实/已否定；
    其余 → 待核验。"""
    if support >= 1 and oppose >= 1:
        return STATUS_DISPUTED
    if support >= 2:
        return STATUS_CONFIRMED
    if oppose >= 2:
        return STATUS_REFUTED
    return STATUS_PENDING


def summary(conn: sqlite3.Connection, claim_id: int) -> dict:
    """汇总状态：两边立场的报告组数 + 即时重算的核验状态。"""
    counts = stance_counts(conn, claim_id)
    return {
        "support": counts["support"],
        "oppose": counts["oppose"],
        "status": status_of(counts["support"], counts["oppose"]),
    }


# ---------------------------------------------------------------- 重置

def reset_reports(conn: sqlite3.Connection, room_id: int) -> int:
    """重置演练：清空全部报告（命题与白名单保留），各命题回到待核验。返回清除条数。"""
    cur = conn.execute("DELETE FROM intel_reports WHERE room_id = ?", (room_id,))
    conn.commit()
    return cur.rowcount
