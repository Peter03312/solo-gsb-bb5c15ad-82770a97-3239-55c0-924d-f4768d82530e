"""响应时效分析 —— 主持人专用的按轮次响应时延统计（不依赖 fastapi，便于测试）。

与 app.main 同一把 state_lock、同一个 SQLite 连接下被调用，本模块不再自行加锁。
统计口径：

- 数据全部来自事件流：调用方在 state_lock 内先固定事件水位 high_water
  （events 表 MAX(id)），本模块只消费 id <= high_water 的事件，
  生成期间到达的新事件绝不混入；
- 复用时钟事件（clock / reset）重建轮次与每个事件的虚拟毫秒——与复盘导出
  共用 app.export.build_timeline 的同一时间线逻辑；暂停期间虚拟时钟冻结，
  暂停时长天然不计入时延；每次 reset 开启新一轮，各轮统计互不混淆，
  重置后从新一轮重新统计；
- 统计单元 = 该轮每一次卡片揭示 × 揭示时刻的目标参与组：全员卡取揭示瞬间
  房间内全部参与组（按 group_added / group_removed 事件重放成员关系，
  成员关系跨重置保留），定向卡只取目标组；
- 首次回执时延 = 本轮该组对该卡首个 acked=true 回执事件的虚拟毫秒 −
  揭示虚拟毫秒（取事件载荷中服务端写入的 revealed_at_ms，与卡片列表口径一致）；
  首次答复时延同理，取本轮首个 answer_updated 事件的虚拟毫秒；
- 撤销回执（acked=false）与修改答复只是时间线上更晚的事件，不影响
  「首次」取值——首次响应时间一旦产生即不可改写；
- 未响应的时延为 None（面板显示「未达」）：绝不拿墙钟或当前时钟补足时延；
- 每组两类中位时延（首次回执 / 首次答复）只取已响应样本，空样本为 None
  （面板显示「无数据」）。
"""

from __future__ import annotations

import sqlite3

from . import export as exportmod


class LatencyError(Exception):
    """统计口径校验错误。main.py 捕获后转成对应 HTTP 状态码（detail 为消息文本）。"""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _median(values: list[int]) -> float | None:
    """中位数：只取已响应样本；空样本返回 None（面板显示「无数据」）。"""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2


def build_latency_report(
    conn: sqlite3.Connection,
    room_id: int,
    round_idx: int,
    high_water: int,
    elapsed_now_ms: int,
) -> dict:
    """按轮次统计各参与组逐卡的首次回执/首次答复虚拟毫秒差。

    调用方必须在 state_lock 内调用，且 high_water 为进入临界区后读取的
    events MAX(id)；elapsed_now_ms 为当前虚拟时钟值（与快照同口径），
    仅用于收尾最后一轮的轮次元数据。
    """
    rows = conn.execute(
        "SELECT id, type, payload, created_at FROM events"
        " WHERE room_id = ? AND id <= ? ORDER BY id",
        (room_id, high_water),
    ).fetchall()
    _, rounds, timeline = exportmod.build_timeline(rows, elapsed_now_ms)
    current = len(rounds) - 1
    if round_idx < 0 or round_idx > current:
        raise LatencyError(404, "round not found")

    # 单次按事件 ID 顺序扫描（时间线顺序与 id 顺序一致，显式排序以绝后患）：
    # 重放组成员关系（跨重置保留），登记本轮揭示的卡片与其揭示时刻目标组，
    # 并记录本轮每个 (卡片, 目标组) 的首个回执 / 首个答复事件的虚拟毫秒。
    members: dict[int, str] = {}                # 重放中的当前参与组（id → 名称）
    last_names: dict[int, str] = {}             # 历史出现过的组名（含已删除组）
    reveals: list[dict] = []                    # 本轮揭示（含揭示时刻目标组快照）
    revealed_keys: set[tuple[int, int]] = set()  # 本轮已揭示卡的 (card_id, 目标组)
    first_receipt: dict[tuple[int, int], int] = {}  # (card_id, group_id) → 首次回执虚拟毫秒
    first_answer: dict[tuple[int, int], int] = {}   # (card_id, group_id) → 首次答复虚拟毫秒

    for it in sorted(timeline, key=lambda x: x["id"]):
        etype = it["type"]
        payload = it["payload"]
        if etype == "group_added":
            members[payload["id"]] = payload.get("name", "")
            last_names[payload["id"]] = payload.get("name", "")
            continue
        if etype == "group_renamed":
            if payload["id"] in members:
                members[payload["id"]] = payload.get("name", "")
            last_names[payload["id"]] = payload.get("name", "")
            continue
        if etype == "group_removed":
            members.pop(payload["id"], None)
            continue
        if it["round"] != round_idx:
            continue  # 揭示/回执/答复只统计所选轮次（重置后从新一轮重新统计）
        if etype == "card_revealed":
            card_id = payload["id"]
            gid = payload.get("group_id")
            # 揭示时刻的目标组：定向卡只取目标组；全员卡取当时全部参与组
            targets = [gid] if gid is not None else sorted(members)
            reveal_ms = payload.get("revealed_at_ms")
            reveals.append({
                "card_id": card_id,
                "seq": payload.get("seq"),
                "title": payload.get("title", ""),
                "card_group_id": gid,
                "revealed_at_ms": reveal_ms if reveal_ms is not None else it["vtime_ms"],
                "targets": targets,
            })
            for target in targets:
                revealed_keys.add((card_id, target))
        elif etype == "receipt_updated":
            if not payload.get("acked"):
                continue  # 撤销回执不影响首次响应时间
            key = (payload.get("card_id"), payload.get("group_id"))
            if key in revealed_keys and key not in first_receipt:
                first_receipt[key] = it["vtime_ms"]
        elif etype == "answer_updated":
            # 修改答复只是更晚的事件，首个 answer_updated 一旦记录即不改写
            key = (payload.get("card_id"), payload.get("group_id"))
            if key in revealed_keys and key not in first_answer:
                first_answer[key] = it["vtime_ms"]

    # 组显示名以当前 groups 表为准（改名后最新），已删除组沿用事件流最后的名字
    existing: set[int] = set()
    for row in conn.execute("SELECT id, name FROM groups WHERE room_id = ?", (room_id,)):
        last_names[row["id"]] = row["name"]
        existing.add(row["id"])

    per_group: dict[int, dict] = {}
    for rev in reveals:
        reveal_ms = rev["revealed_at_ms"]
        for gid in rev["targets"]:
            key = (rev["card_id"], gid)
            rec = first_receipt.get(key)
            ans = first_answer.get(key)
            entry = per_group.setdefault(gid, {"cards": [], "receipt_lat": [], "answer_lat": []})
            entry["cards"].append({
                "card_id": rev["card_id"],
                "seq": rev["seq"],
                "title": rev["title"],
                "card_group_id": rev["card_group_id"],  # None = 全员卡
                "revealed_at_ms": reveal_ms,
                # 未响应为 None（未达），不用墙钟或当前时钟补足时延
                "receipt_latency_ms": None if rec is None else max(0, rec - reveal_ms),
                "answer_latency_ms": None if ans is None else max(0, ans - reveal_ms),
            })
            if rec is not None:
                entry["receipt_lat"].append(max(0, rec - reveal_ms))
            if ans is not None:
                entry["answer_lat"].append(max(0, ans - reveal_ms))

    groups_out = []
    for gid in sorted(per_group):
        entry = per_group[gid]
        groups_out.append({
            "group_id": gid,
            "group_name": last_names.get(gid) or f"#{gid}",
            "exists": gid in existing,  # False = 该组已被删除（历史数据仍保留）
            "receipt_median_ms": _median(entry["receipt_lat"]),
            "answer_median_ms": _median(entry["answer_lat"]),
            "receipt_responded": len(entry["receipt_lat"]),
            "answer_responded": len(entry["answer_lat"]),
            "cards": entry["cards"],
        })

    return {
        "round": round_idx,
        "current_round": current,
        "high_water_event_id": high_water,
        "groups": groups_out,
    }
