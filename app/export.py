"""复盘包导出：主持人可在任意时刻按「当前事件水位」生成自包含 ZIP。

一致性：调用方（main.py 的路由）必须在 state_lock 内、与所有写操作同一把锁
先读取水位 high_water（events 表 MAX(id)），本模块只采集 id <= high_water 的
事件与当前状态行——快照读取期间即使有写者排队等待，其新事件 id 必然大于水位，
因此生成期间到达的新事件绝不会混入复盘包。

包内内容（JSON，均已脱敏：不含房间码、组加入码、观察 token）：
- generated 元信息与事件水位
- orchestration：房间编排（参与组、全部卡片及门槛配置）
- revealed_cards：水位时刻已揭示卡片
- receipts：水位时刻各卡各组回执
- answers：水位时刻各卡各组最新答复
- answer_versions：答复版本轨迹（由事件流重放得到，含历史版本正文）
- anchors：复盘锚点（仅含 created_event_id <= 导出水位、即导出前已创建者，按轮次/虚拟毫秒排序）
- clock_actions：时钟动作轨迹（开始/暂停/继续/重置，附虚拟时间）
- events：脱敏后的完整事件流（供离线回放页重建任意虚拟时刻状态）

回放页 replay.html 为单文件、零外链，双击即可在浏览器离线打开，
可拖动虚拟时间还原每一轮演练当时的状态；同一虚拟时刻的事件按事件 ID 稳定排序；
点击某轮锚点可跳到其绑定虚拟毫秒，并只重放至锚点绑定水位，还原同一虚拟时刻内
按事件 ID 严格截断的精确状态。
"""

from __future__ import annotations

import io
import json
import re
import sqlite3
import time
import zipfile
from pathlib import Path

PACKAGE_FORMAT_VERSION = 2  # v2：新增 anchors（复盘锚点）；v1 旧包回放页仍可打开
REPLAY_HTML_NAME = "replay.html"
DATA_JSON_NAME = "data.json"

_TEMPLATE = (Path(__file__).resolve().parent / "static" / "replay_template.html").read_text(
    encoding="utf-8"
)
# 注入标记同时出现在模板的 <script> 与 <noscript> 中；JSON 内已转义 < / >，
# 替换不会与数据内容冲突。
_DATA_MARKER = '"__REPLAY_DATA_JSON__"'
_NOSCRIPT_MARKER = "__REVIEW_DATA_SUMMARY__"


# ---------------------------------------------------------------- 时间线 / 轮次

def build_timeline(events: list[sqlite3.Row], elapsed_now_ms: int):
    """把事件流加工为「时钟动作」「轮次元数据」与「按轮次定位的事件时间线」。

    每次 reset 结束当前轮次并开启新一轮：编排（组/卡片）跨轮次保留，
    揭示、回执、答复按轮次清零。事件的虚拟时间 vtime_ms 由其发生时
    时钟所处的运行段推导——运行中事件挂靠当前运行段起点（记录该段
    开始的墙钟时刻），暂停中事件取冻结值；时钟/重置动作自身带权威
    elapsed_ms。

    本函数同时被 app.latency（响应时效面板）复用：同一份时钟事件重建逻辑，
    保证复盘导出与响应时延统计的轮次划分、虚拟时间口径完全一致。
    """
    clock_actions: list[dict] = []
    timeline: list[dict] = []

    round_idx = 0
    run_start_ms: int | None = None     # 当前运行段起点虚拟毫秒；None = 未运行
    run_start_wall: float | None = None  # 当前运行段起点墙钟（秒）
    run_frozen_ms = 0                    # 最近一次暂停/重置结算出的冻结值
    rounds: list[dict] = [{"round": 0, "ended": False, "end_ms": None}]

    def vtime_at(wall: float) -> int:
        if run_start_ms is not None and run_start_wall is not None:
            return max(0, run_start_ms + int((wall - run_start_wall) * 1000))
        return max(0, run_frozen_ms)

    for ev in events:
        etype = ev["type"]
        payload = json.loads(ev["payload"])
        wall = ev["created_at"]

        if etype == "clock":
            action = payload.get("action", "")
            elapsed = int(payload.get("elapsed_ms") or 0)
            if action == "started":
                run_start_ms, run_start_wall = 0, wall
                run_frozen_ms = 0
            elif action == "paused":
                run_start_ms, run_start_wall = None, None
                run_frozen_ms = elapsed
            elif action == "resumed":
                run_start_ms, run_start_wall = elapsed, wall
                run_frozen_ms = elapsed
            clock_actions.append({
                "event_id": ev["id"],
                "round": round_idx,
                "action": action,
                "status": payload.get("status", ""),
                "vtime_ms": elapsed,
                "created_at": wall,
            })
            vtime = elapsed
            is_clock = True
        elif etype == "reset":
            # 重置事件本身标记上一轮的终点（T+ 冻结值），随后开启新一轮
            prev_vtime = vtime_at(wall)
            rounds[round_idx]["ended"] = True
            rounds[round_idx]["end_ms"] = prev_vtime
            clock_actions.append({
                "event_id": ev["id"],
                "round": round_idx,
                "action": "reset",
                "status": "idle",
                "vtime_ms": prev_vtime,
                "created_at": wall,
            })
            vtime, is_clock = prev_vtime, True
            round_idx += 1
            run_start_ms, run_start_wall = None, None
            run_frozen_ms = 0
            rounds.append({"round": round_idx, "ended": False, "end_ms": None})
        else:
            vtime, is_clock = vtime_at(wall), False

        timeline.append({
            "id": ev["id"],
            "type": etype,
            "payload": payload,
            "created_at": wall,
            "round": round_idx,
            "vtime_ms": vtime,
            "is_clock": is_clock,
        })

    # 当前（最后一个）轮次的跨度：以水位时刻虚拟时钟值收尾；
    # 已结束轮次的 end_ms 在 reset 时记录。
    rounds[-1]["end_ms"] = max(elapsed_now_ms, 0)
    _sort_timeline(timeline, rounds)
    return clock_actions, rounds, timeline


def _sort_timeline(timeline: list[dict], rounds: list[dict]) -> None:
    """为时间线事件附加轮次结束标记，并按 (round, vtime_ms, id) 稳定原地排序——
    同一虚拟时刻的事件严格按事件 ID 排序。"""
    for it in timeline:
        meta = rounds[it["round"]] if it["round"] < len(rounds) else {"ended": False}
        it["round_ended"] = bool(meta.get("ended"))
    timeline.sort(key=lambda x: (x["round"], x["vtime_ms"], x["id"]))


# ---------------------------------------------------------------- 脱敏事件

# 可进入复盘包的事件类型白名单；share_*（含观察 token）一律剔除。
# anchor_created/anchor_updated/anchor_deleted 同样不进事件流：锚点由独立的
# anchors 表导出（按绑定水位截断），无需在脱敏事件时间线中重放。
_EXPORTED_TYPES = {
    "group_added", "group_renamed", "group_removed",
    "card_added", "card_removed", "card_revealed",
    "receipt_updated",
    "answer_updated", "answer_lock_updated",
    "gate_updated",
    "clock", "reset",
}


def _sanitize_event(etype: str, payload: dict) -> dict | None:
    """事件脱敏：剔除白名单外事件（share_* 等）与组加入码字段。

    房间码从不出现在任何事件载荷中；观察 token 只存在于 share_* 事件，
    整类不导出。
    """
    if etype not in _EXPORTED_TYPES:
        return None
    clean = dict(payload)
    if etype == "group_added":
        clean.pop("join_code", None)  # 组加入码绝不进复盘包
    return clean


# ---------------------------------------------------------------- 快照采集

def _orchestration(conn: sqlite3.Connection, room_id: int, room: sqlite3.Row) -> dict:
    """房间编排（主持人视角）：组（无加入码）、全部卡片与门槛配置。"""
    groups = conn.execute(
        "SELECT * FROM groups WHERE room_id = ? ORDER BY id", (room_id,)
    ).fetchall()
    cards = conn.execute(
        "SELECT * FROM cards WHERE room_id = ? ORDER BY trigger_seconds, seq", (room_id,)
    ).fetchall()
    group_data = [{"id": g["id"], "name": g["name"]} for g in groups]
    card_data = []
    for c in cards:
        dep = None
        if c["depends_on_card_id"] is not None:
            d = conn.execute(
                "SELECT seq FROM cards WHERE id = ?", (c["depends_on_card_id"],)
            ).fetchone()
            dep = {"depends_on_card_id": c["depends_on_card_id"],
                   "depends_on_seq": d["seq"] if d else None,
                   "gate_timeout_seconds": c["gate_timeout_seconds"]}
        card_data.append({
            "id": c["id"],
            "seq": c["seq"],
            "trigger_seconds": c["trigger_seconds"],
            "title": c["title"],
            "content": c["content"],
            "group_id": c["group_id"],
            "revealed": bool(c["revealed"]),
            "revealed_at_ms": c["revealed_at_ms"],
            "timed_out": bool(c["timed_out"]),
            "answers_locked": bool(c["answers_locked"]),
            "gate": dep,
        })
    return {
        "room": {
            "name": room["name"],
            "status": room["status"],
            "elapsed_ms": _elapsed_ms(room),
        },
        "groups": group_data,
        "cards": card_data,
    }


def _elapsed_ms(room: sqlite3.Row) -> int:
    """与 main.room_elapsed_ms 同口径：running 时叠加本次运行段墙钟流逝。"""
    elapsed = room["elapsed_ms"]
    if room["status"] == "running" and room["last_resume_at"] is not None:
        elapsed += int((time.time() - room["last_resume_at"]) * 1000)
    return max(0, elapsed)


def _revealed_cards(conn: sqlite3.Connection, room_id: int) -> list[dict]:
    rows = conn.execute(
        """SELECT id, seq, trigger_seconds, title, content, group_id,
                  revealed_at_ms, timed_out, answers_locked
           FROM cards WHERE room_id = ? AND revealed = 1
           ORDER BY trigger_seconds, seq""",
        (room_id,),
    ).fetchall()
    return [{
        "id": r["id"],
        "seq": r["seq"],
        "trigger_seconds": r["trigger_seconds"],
        "title": r["title"],
        "content": r["content"],
        "group_id": r["group_id"],
        "revealed_at_ms": r["revealed_at_ms"],
        "timed_out": bool(r["timed_out"]),
        "answers_locked": bool(r["answers_locked"]),
    } for r in rows]


def _receipts(conn: sqlite3.Connection, room_id: int) -> list[dict]:
    rows = conn.execute(
        """SELECT card_id, group_id, created_at
           FROM receipts WHERE room_id = ?
           ORDER BY card_id, group_id""",
        (room_id,),
    ).fetchall()
    return [{"card_id": r["card_id"], "group_id": r["group_id"],
             "created_at": r["created_at"]} for r in rows]


def _answers(conn: sqlite3.Connection, room_id: int) -> list[dict]:
    rows = conn.execute(
        """SELECT card_id, group_id, body, revision, updated_at
           FROM answers WHERE room_id = ?
           ORDER BY card_id, group_id""",
        (room_id,),
    ).fetchall()
    return [{"card_id": r["card_id"], "group_id": r["group_id"], "body": r["body"],
             "revision": r["revision"], "updated_at": r["updated_at"]} for r in rows]


def _answer_versions(conn: sqlite3.Connection, room_id: int, high_water: int) -> list[dict]:
    """答复版本轨迹：重放 answer_updated 事件（id <= 水位），按卡×组保留全部版本。

    每次 PUT 成功都会落一条事件（revision 从 1 起递增），故事件流即完整版本史；
    reset 后新一轮从 revision 1 重新计数，用 round 标记区分（由时间线推导）。
    """
    rows = conn.execute(
        """SELECT id, payload FROM events
           WHERE room_id = ? AND type = 'answer_updated' AND id <= ?
           ORDER BY id""",
        (room_id, high_water),
    ).fetchall()
    versions: list[dict] = []
    for r in rows:
        p = json.loads(r["payload"])
        versions.append({
            "event_id": r["id"],
            "card_id": p.get("card_id"),
            "group_id": p.get("group_id"),
            "body": p.get("body", ""),
            "revision": p.get("revision"),
            "updated_at": p.get("updated_at"),
        })
    return versions


def _anchors(conn: sqlite3.Connection, room_id: int, high_water: int) -> list[dict]:
    """复盘锚点：只导出 created_event_id <= 导出水位的锚点（锚点已在水位前创建）。

    锚点在 state_lock 内绑定「其自身创建事件落库前」的事件水位（event_id），
    并记录自身创建事件 id（created_event_id，恰为 event_id + 1）。按
    created_event_id <= high_water 截断后，临界区之外（生成期间）新建的锚点
    ——其创建事件 id 必然大于水位——绝不会混入；即便手动给定任意水位也成立。
    锚点行跨重置保留，行内 round 决定其在回放页归属的演练轮次；
    回放还原时按 event_id（绑定水位，而非 created_event_id）截断事件；
    备注修改（updated_at）导出最新值，但绑定的 round/vtime_ms/event_id 永不变。
    """
    rows = conn.execute(
        """SELECT id, title, note, round, vtime_ms, event_id, created_event_id,
                  created_at, updated_at
           FROM anchors
           WHERE room_id = ? AND created_event_id <= ?
           ORDER BY round, vtime_ms, id""",
        (room_id, high_water),
    ).fetchall()
    return [{
        "id": r["id"],
        "title": r["title"],
        "note": r["note"],
        "round": r["round"],
        "vtime_ms": r["vtime_ms"],
        "event_id": r["event_id"],
        "created_event_id": r["created_event_id"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    } for r in rows]


# ---------------------------------------------------------------- 组装

def build_review_data(conn: sqlite3.Connection, room: sqlite3.Row, high_water: int) -> dict:
    """采集水位时刻的完整复盘数据。调用方保证只在 state_lock 下调用，
    且 high_water 为进入临界区后读取的 events MAX(id)。"""
    room_id = room["id"]

    rows = conn.execute(
        "SELECT id, type, payload, created_at FROM events"
        " WHERE room_id = ? AND id <= ? ORDER BY id",
        (room_id, high_water),
    ).fetchall()
    # 脱敏 + 白名单过滤后的水位内事件（回放页只消费这份，不接触任何凭证）
    kept_rows = []
    for ev in rows:
        payload = json.loads(ev["payload"])
        if _sanitize_event(ev["type"], payload) is None:
            continue
        kept_rows.append(ev)

    elapsed_now = _elapsed_ms(room)
    clock_actions, rounds_meta, timeline = build_timeline(kept_rows, elapsed_now)

    # 事件时间线对外只暴露脱敏载荷（与 _sanitize_event 同一口径）
    timeline_out = [{
        "id": it["id"],
        "type": it["type"],
        "payload": _sanitize_event(it["type"], it["payload"]) or {},
        "created_at": it["created_at"],
        "round": it["round"],
        "vtime_ms": it["vtime_ms"],
        "is_clock": it["is_clock"],
    } for it in timeline]

    return {
        "format": "inject-review",
        "format_version": PACKAGE_FORMAT_VERSION,
        "generated_at": time.time(),
        "high_water_event_id": high_water,
        "room": {"name": room["name"], "status": room["status"], "elapsed_ms": elapsed_now},
        "rounds": rounds_meta,
        "orchestration": _orchestration(conn, room_id, room),
        "revealed_cards": _revealed_cards(conn, room_id),
        "receipts": _receipts(conn, room_id),
        "answers": _answers(conn, room_id),
        "answer_versions": _answer_versions(conn, room_id, high_water),
        "anchors": _anchors(conn, room_id, high_water),
        "clock_actions": clock_actions,
        "events": timeline_out,
    }


def safe_filename(name: str) -> str:
    stem = re.sub(r"[\\/:*?\"<>|\s]+", "_", (name or "").strip()).strip("_")
    return stem[:80] or "drill"


def review_filename(room_name: str, now: float | None = None) -> str:
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now or time.time()))
    return f"review-{safe_filename(room_name)}-{stamp}.zip"


def render_review_html(data: dict) -> str:
    """把复盘数据内联进单文件回放页（零外链、可离线打开）。"""
    # JSON 内嵌到 <script>：转义 < / > 防止内容截断脚本；U+2028/2029 为
    # JSON 合法但旧 JS 非法的行分隔，一并转义。
    payload = json.dumps(data, ensure_ascii=False).replace("<", "\\u003c").replace(
        ">", "\\u003e").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    summary = (f"{data['room'].get('name') or '未命名演练'} · "
               f"事件水位 {data['high_water_event_id']} · "
               f"卡片 {len(data['orchestration']['cards'])} 张 · "
               f"锚点 {len(data.get('anchors') or [])} 个 · "
               f"{len(data['clock_actions'])} 个时钟动作")
    return (_TEMPLATE
            .replace(_DATA_MARKER, payload, 1)
            .replace(_NOSCRIPT_MARKER, summary, 1))


def build_review_zip(data: dict) -> bytes:
    """生成 ZIP 字节：replay.html（离线回放页）+ data.json（原始复盘数据）。"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(REPLAY_HTML_NAME, render_review_html(data))
        zf.writestr(DATA_JSON_NAME,
                    json.dumps(data, ensure_ascii=False, indent=2))
        zf.writestr("README.txt", _README_TXT)
    return buf.getvalue()


_README_TXT = """\
情景注入台 · 演练复盘包
========================

本压缩包为离线自包含复盘材料，无需联网、无需服务端：

- replay.html  双击用浏览器打开即可进入回放页（所有资源均已内联，零外链）。
               顶部可切换演练轮次、拖动「虚拟时间」滑块还原当时状态；
               同一虚拟时刻的事件按事件 ID 稳定排序。
               「复盘锚点」按轮次列出：点击锚点后滑块跳到其绑定的虚拟时间，
               并仅重放至锚点绑定的事件水位，还原该虚拟时刻内的精确状态
               （同一虚拟毫秒内、ID 晚于锚点水位的事件不计入）。
- data.json    复盘原始数据：房间编排、已揭示卡片、回执、答复版本轨迹、
               复盘锚点（不晚于导出水位）、时钟动作与脱敏事件流
               （不含房间码、组加入码、观察 token）。
"""
