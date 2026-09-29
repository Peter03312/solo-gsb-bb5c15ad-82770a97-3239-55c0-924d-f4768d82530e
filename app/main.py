"""情景注入台 —— FastAPI 主程序。

职责：
- 房间 / 参与组 / 注入卡片的 REST 管理
- 可暂停 / 继续的虚拟时钟（暂停期间绝不推进）
- 后台 tick：按 (trigger_seconds, seq) 稳定次序揭示到期卡片
- 卡片回执：参与者对已揭示的本组可见卡片标记/撤销「已收到」，
  同一参与组仅保留一条回执；主持人实时看到每组回执/未回执汇总
- 回执门槛：卡片可依赖一张前置卡片，前置卡片对其目标参与组全部集齐回执后才揭示；
  可配虚拟时钟超时秒数（自触发时刻起算），超时仍未集齐则标记为超时揭示并继续；
  门槛状态由持久化的回执与卡片字段推导，暂停/继续、重置、重连与重启后均保持
- 限时观察席分享：主持人选择 1~3 个参与组并设置 5~120 分钟有效期，
  生成可撤销的随机观察链接；观察者只读，仅能收到已揭示的全员卡片与
  所选组卡片，拿不到未揭示卡、房间码、组加入码、回执明细与控制能力；
  到期或撤销时立即断开在线观察者，凭证此后一律失效
- 复盘锚点：主持人在任意状态记录标题/备注，服务端在同一把状态锁内把锚点
  绑定到当前轮次、虚拟毫秒与当时事件水位（暂停期间冻结）；支持改备注/删除，
  实时同步给主持人，参与者与观察者不可见；重置后锚点保留并归属原轮次；
  导出复盘包按锚点创建事件水位截断，离线回放页可按锚点精确还原当时状态
- 处置答复：参与者对本组可见且已揭示的卡片提交/修改一份组内答复，
  更新必须携带当前 revision，版本过期返回 409 与最新答复（防同组多窗口
  静默覆盖）；主持人可逐卡锁定/重新开放（锁定后参与者只读、写入 409），
  并实时看到各应答组的提交状态、正文与版本（定向卡仅目标组，全员卡覆盖
  现有全部组）；答复持久化，经快照与 WebSocket 恢复，参与者看不到他组
  正文；重置演练时清空答复并解除锁定
- 现场决策投票：主持人创建含 2～4 个选项、指定参与组与截止虚拟秒的投票；
  仅指定组可在截止前提交/改投（每组只计一票）。后台 tick 在状态锁内对
  到期投票一次性结算：最高票获胜，平票按选项创建顺序决定；结算当时把
  各选项票数、逐组选票与获胜项在同一事务固定到 polls/poll_results，此后
  删组（级联残余选票）、刷新或重启都展示结算当时的一致结果，不用残余票
  重算；未结算投票删组仍级联资格与选票、实时重算。旧库已结算行启动时从
  poll_settled 事件恢复固定票数，事件缺失只保留获胜项并标示票数不可恢复。
  暂停冻结截止计时，重连与重启恢复状态。主持人实时查看逐组选票与汇总，
  参与者结算前只见自己的选择、结算后才看到固定结果，观察者完全不可见；
  重置时未结算投票作废，已结算结果按轮次保留
- 应急车辆申请：主持人为房间创建一个总量固定（1～20 辆）的车辆池；
  各参与组凭加入码提交一份申请（1～5 辆且不超过总量，取消后才能重新申请）。
  服务端在 state_lock 内按受理顺序处理：无更早待分配申请且余量足够才立即
  分配，否则入队；取消已分配申请释放车辆后从队首依次补位，队首数量不足时
  不跳过。主持人看到各组申请、队列与剩余车辆；参与组只看到本组申请状态与
  排位；观察席不接收申请。申请与分配结果持久化，经快照与事件恢复；
  重置演练清空申请并恢复全部余量，车辆池保留
- 现场情报核验板：主持人创建待核验命题并指定至少两个参与组；白名单内
  参与组凭加入码提交「支持/反对 + 简短依据」，同组修改覆盖原报告、也可撤回。
  服务端只按不同参与组的最新报告即时重算状态（UNIQUE(claim, group)，
  同组多次提交绝不计为多份证据）：两边均有报告为有争议；同一立场至少两组
  且对立为零为已证实/已否定；其余为待核验。主持人实时看到逐组报告；
  参与组只见本组报告与汇总状态；观察席完全不可见。报告持久化，
  经快照与 WebSocket 恢复；重置清空报告但保留命题
- 响应时效面板（仅主持人）：按演练轮次统计每张卡片揭示时各目标组到
  首次回执、首次答复的虚拟毫秒差；服务端在 state_lock 内固定事件水位后
  复用时钟事件重建轮次与虚拟时间（暂停时长不计入，重置后从新一轮统计）；
  撤销回执或修改答复不改首次响应时间；未响应记为未达，不用墙钟补足时延；
  逐组给出两类中位时延（只取已响应样本，空样本为无数据）；
  参与者与观察席拿不到分析数据
- WebSocket：先下发权威快照，再广播单调递增的增量事件
- 分组定向：参与者凭组加入码连接，快照与后续事件都按该组过滤；
  未命中的卡片在触发前后都不通过 REST 或 WebSocket 下发，主持人始终可见全部
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import secrets
import sqlite3
import time
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path
from typing import NamedTuple

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db as dbmod
from . import export as reviewmod
from . import intel as intelmod
from . import latency as latencymod
from . import polls as pollmod
from . import vehicles as vehiclemod

log = logging.getLogger("inject")
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TICK_INTERVAL = float(os.environ.get("TICK_INTERVAL", "0.25"))
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 去掉易混淆字符
MAX_GROUPS = 8  # 每个房间最多 8 个参与组

db: sqlite3.Connection
# 所有读改写 + 广播都在这把锁下串行，保证“快照先于其后发生的事件”这一顺序。
state_lock = asyncio.Lock()


# ---------------------------------------------------------------- 工具

def now_ms() -> int:
    return int(time.time() * 1000)


def room_elapsed_ms(room: sqlite3.Row) -> int:
    """虚拟时钟当前值：running 时 = 冻结值 + 本次运行段，否则 = 冻结值。"""
    elapsed = room["elapsed_ms"]
    if room["status"] == "running" and room["last_resume_at"] is not None:
        elapsed += int((time.time() - room["last_resume_at"]) * 1000)
    return max(0, elapsed)


def room_public(room: sqlite3.Row, *, include_code: bool = True) -> dict:
    data = {
        "name": room["name"],
        "status": room["status"],
        "elapsed_ms": room_elapsed_ms(room),
        "server_now_ms": now_ms(),
    }
    if include_code:
        # 房间码即主持人凭证，绝不随参与者快照下发
        data["code"] = room["code"]
    return data


def group_public(group: sqlite3.Row) -> dict:
    return {
        "id": group["id"],
        "name": group["name"],
        "join_code": group["join_code"],
    }


def card_public(card: sqlite3.Row) -> dict:
    return {
        "id": card["id"],
        "seq": card["seq"],
        "trigger_seconds": card["trigger_seconds"],
        "title": card["title"],
        "content": card["content"],
        "group_id": card["group_id"],  # None = 全员
        "revealed": bool(card["revealed"]),
        "revealed_at_ms": card["revealed_at_ms"],
        "answers_locked": bool(card["answers_locked"]),  # 答复锁定状态（不含任何答复内容）
    }


def answer_public(answer: sqlite3.Row) -> dict:
    """一份组内答复的序列化。含正文，只随主持人/本组视野下发，绝不给他组与观察者。"""
    return {
        "card_id": answer["card_id"],
        "group_id": answer["group_id"],
        "body": answer["body"],
        "revision": answer["revision"],
        "updated_at": answer["updated_at"],
    }


def current_round(room_id: int) -> int:
    """当前演练轮次 = 此前 reset 事件条数：每次重置结束上一轮并开启新一轮。

    与 export 时间线（按 reset 切轮）同一口径；须在 state_lock 下调用。
    """
    return db.execute(
        "SELECT COUNT(*) AS c FROM events WHERE room_id = ? AND type = 'reset'",
        (room_id,),
    ).fetchone()["c"]


def anchor_public(anchor: sqlite3.Row) -> dict:
    """复盘锚点序列化。仅主持人可见：快照与 anchor_* 事件都按主持人视野过滤。"""
    return {
        "id": anchor["id"],
        "title": anchor["title"],
        "note": anchor["note"],
        "round": anchor["round"],
        "vtime_ms": anchor["vtime_ms"],
        "event_id": anchor["event_id"],   # 绑定水位：导出与离线回放截断到该事件
        "created_event_id": anchor["created_event_id"],  # 自身创建事件 id
        "created_at": anchor["created_at"],
        "updated_at": anchor["updated_at"],
    }


# ---------------------------------------------------------------- 现场决策投票

def poll_option_public(option: sqlite3.Row) -> dict:
    return {"id": option["id"], "seq": option["seq"], "text": option["text"]}


def poll_host_view(poll: sqlite3.Row) -> dict:
    """主持人视野：选项、参与组白名单、逐组选票与选项汇总，全量下发。

    选票明细（哪一组投了哪个选项）只随主持人快照/事件下发；
    参与者视野走 poll_player_view，观察者完全拿不到投票。

    已结算投票的 tallies/votes 一律取结算当时固定的 poll_results 快照：
    此后删除参与组虽级联删除 poll_votes 残余行，主持人看到的仍是结算时的
    结果（获胜项必有固定票数），绝不拿残余选票重算。旧库恢复时若结算事件
    缺失，则 tallies/votes 为 null 且 tallies_recoverable=false（获胜项仍
    保留），明确标示票数不可恢复。open/voided 投票仍按当前残余行实时呈现。
    """
    options = pollmod.poll_options(db, poll["id"])
    group_ids = pollmod.poll_group_ids(db, poll["id"])
    data = {
        "id": poll["id"],
        "question": poll["question"],
        "round": poll["round"],
        "deadline_ms": poll["deadline_ms"],
        "status": poll["status"],               # open | settled | voided
        "group_ids": group_ids,
        "options": [poll_option_public(o) for o in options],
        "winner_option_id": poll["winner_option_id"],
        "settled_at_ms": poll["settled_at_ms"],
    }
    if poll["status"] == "settled":
        fixed = pollmod.fixed_result(db, poll["id"])
        if fixed is not None and fixed.recoverable:
            # 以选项列表为序固定汇总（固定快照含全部选项，含 0 票者）
            fixed_tallies: dict = {}
            for t in fixed.tallies:
                if isinstance(t, dict) and "option_id" in t:
                    fixed_tallies[t["option_id"]] = t.get("count", 0)
            data["tallies"] = [
                {"option_id": opt["id"], "count": fixed_tallies.get(opt["id"], 0)}
                for opt in options
            ]
            data["votes"] = [
                {"group_id": v["group_id"], "option_id": v["option_id"]}
                for v in fixed.votes
                if isinstance(v, dict) and "group_id" in v and "option_id" in v
            ]
            data["tallies_recoverable"] = True
        else:
            # 理论不该发生（新结算同事务必写快照）；旧库结算事件缺失时落入此分支
            data["tallies"] = None
            data["votes"] = None
            data["tallies_recoverable"] = False
        return data
    # open：实时逐组选票；voided：重置时选票已删（二者都不存在固定快照）
    votes = []
    tallies = {opt["id"]: 0 for opt in options}
    for row in db.execute(
        "SELECT group_id, option_id FROM poll_votes WHERE poll_id = ? ORDER BY group_id",
        (poll["id"],),
    ):
        votes.append({"group_id": row["group_id"], "option_id": row["option_id"]})
        if row["option_id"] in tallies:
            tallies[row["option_id"]] += 1
    data["votes"] = votes
    data["tallies"] = [
        {"option_id": opt["id"], "count": tallies[opt["id"]]} for opt in options
    ]
    return data


def poll_player_view(poll: sqlite3.Row, group_id: int) -> dict:
    """参与者视野：结算前只见自己的选择（无任何票数/他组信息），
    结算后才看到获胜选项与选项汇总；作废投票只见作废状态。

    已结算投票的票数同样取固定快照，与主持人看到的票数严格一致；固定快照
    不可恢复（旧库缺结算事件）时 tallies 为 null 且 tallies_recoverable=false，
    不使用残余选票伪造。本组已投选项结算后也以固定快照为准（删组不影响仍有
    资格的他组；本组若被删则白名单已不再包含本组，不会进入本视图）。"""
    options = pollmod.poll_options(db, poll["id"])
    data = {
        "id": poll["id"],
        "question": poll["question"],
        "round": poll["round"],
        "deadline_ms": poll["deadline_ms"],
        "status": poll["status"],
        "options": [poll_option_public(o) for o in options],
        "my_option_id": None,
    }
    if poll["status"] == "settled":
        fixed = pollmod.fixed_result(db, poll["id"])
        my_option_id = None
        if fixed is not None and fixed.recoverable:
            my_option_id = next(
                (v.get("option_id") for v in fixed.votes
                 if isinstance(v, dict) and v.get("group_id") == group_id),
                None,
            )
            fixed_tallies: dict = {}
            for t in fixed.tallies:
                if isinstance(t, dict) and "option_id" in t:
                    fixed_tallies[t["option_id"]] = t.get("count", 0)
            data["tallies"] = [
                {"option_id": opt["id"], "count": fixed_tallies.get(opt["id"], 0)}
                for opt in options
            ]
            data["tallies_recoverable"] = True
        else:
            # 快照不可恢复：本组选择同样不可信（残余票可能已被删），不回填
            data["tallies"] = None
            data["tallies_recoverable"] = False
        data["my_option_id"] = my_option_id
        data["winner_option_id"] = poll["winner_option_id"]
        data["settled_at_ms"] = poll["settled_at_ms"]
        return data
    vote = pollmod.vote_of_group(db, poll["id"], group_id)
    data["my_option_id"] = vote["option_id"] if vote is not None else None
    return data


# ---------------------------------------------------------------- 现场情报核验板

def intel_report_public(report: sqlite3.Row) -> dict:
    """一份组报告的序列化。含依据正文，只随主持人/本组视野下发，绝不给他组与观察者。"""
    return {
        "group_id": report["group_id"],
        "stance": report["stance"],          # support | oppose
        "rationale": report["rationale"],
        "revision": report["revision"],
        "updated_at": report["updated_at"],
    }


def intel_host_view(claim: sqlite3.Row) -> dict:
    """主持人视野：命题、白名单、逐组最新报告与汇总状态，全量下发。

    逐组报告（哪一组持什么立场、依据正文）只随主持人快照/事件下发；
    参与组视野走 intel_player_view（本组报告 + 汇总状态），观察者完全拿不到。
    """
    group_ids = intelmod.claim_group_ids(db, claim["id"])
    reports = [intel_report_public(r) for r in intelmod.reports_of_claim(db, claim["id"])]
    return {
        "id": claim["id"],
        "statement": claim["statement"],
        "round": claim["round"],
        "group_ids": group_ids,
        "reports": reports,                  # 逐组最新报告（每组至多一份）
        "summary": intelmod.summary(db, claim["id"]),
        "created_at": claim["created_at"],
    }


def intel_player_view(claim: sqlite3.Row, group_id: int | None) -> dict:
    """参与组视野：命题 + 本组报告 + 汇总状态（支持/反对组数与核验状态）。

    汇总状态只含计数与状态，不含任何逐组立场明细与他组依据正文。
    group_id 为 None 时用于创建广播的通用脱敏载荷（尚无本组报告）。
    """
    mine = (intelmod.report_of_group(db, claim["id"], group_id)
            if group_id is not None else None)
    return {
        "id": claim["id"],
        "statement": claim["statement"],
        "round": claim["round"],
        "group_ids": intelmod.claim_group_ids(db, claim["id"]),
        "my_report": intel_report_public(mine) if mine is not None else None,
        "summary": intelmod.summary(db, claim["id"]),
    }


def get_room_or_404(code: str) -> sqlite3.Row:
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code.upper(),)).fetchone()
    if room is None:
        raise HTTPException(status_code=404, detail="room not found")
    return room
def get_group_or_404(room_id: int, group_id: int) -> sqlite3.Row:
    group = db.execute(
        "SELECT * FROM groups WHERE id = ? AND room_id = ?", (group_id, room_id)
    ).fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="group not found")
    return group


def code_taken(code: str) -> bool:
    """房间码与组加入码共用同一命名空间，互不可重复。"""
    if db.execute("SELECT 1 FROM rooms WHERE code = ?", (code,)).fetchone():
        return True
    return db.execute("SELECT 1 FROM groups WHERE join_code = ?", (code,)).fetchone() is not None


def fresh_code() -> str:
    while True:
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(6))
        if not code_taken(code):
            return code


def resolve_code(code: str) -> tuple[sqlite3.Row | None, int | None]:
    """把连接用码解析为 (房间, 视野)。视野 None = 主持人（全部可见），否则为参与组 id。"""
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code,)).fetchone()
    if room is not None:
        return room, None
    group = db.execute("SELECT * FROM groups WHERE join_code = ?", (code,)).fetchone()
    if group is not None:
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        return room, group["id"]
    return None, None


# ---------------------------------------------------------------- 回执门槛

def gate_pending_groups(card: sqlite3.Row, dep: sqlite3.Row) -> list[int]:
    """前置卡片尚未集齐回执的目标参与组 id 列表；空列表 = 门槛已满足。

    期望回执组与回执汇总同一口径：前置为全员卡取房间全部参与组，
    定向卡只取目标组；已回执组现查 receipts 表（回执持久化在 SQLite，
    暂停/重连/重启后门槛状态不丢）。
    """
    if dep["group_id"] is not None:
        expected = [dep["group_id"]]
    else:
        expected = [r["id"] for r in db.execute(
            "SELECT id FROM groups WHERE room_id = ? ORDER BY id", (card["room_id"],)
        )]
    if not expected:
        return []
    acked = {r["group_id"] for r in db.execute(
        "SELECT group_id FROM receipts WHERE card_id = ?", (dep["id"],)
    )}
    return [gid for gid in expected if gid not in acked]


def gate_view(card: sqlite3.Row) -> dict | None:
    """主持人视野的回执门槛状态；无门槛卡片返回 None（不下发该字段）。

    state：waiting（等待前置集齐回执）| unlocked（门槛已解锁）| timeout（超时揭示）。
    门槛细节只给主持人：参与者/观察者按既有视野过滤，不感知编排依赖。
    """
    dep_id = card["depends_on_card_id"]
    if dep_id is None:
        return None
    dep = db.execute("SELECT * FROM cards WHERE id = ?", (dep_id,)).fetchone()
    pending = gate_pending_groups(card, dep)
    if card["revealed"]:
        state = "timeout" if card["timed_out"] else "unlocked"
    else:
        state = "waiting" if pending else "unlocked"
    return {
        "depends_on_card_id": dep_id,
        "depends_on_seq": dep["seq"],
        "gate_timeout_seconds": card["gate_timeout_seconds"],
        "state": state,
        "pending_groups": pending,  # 尚未回执的前置目标组
    }


async def publish_gate_refresh(room: sqlite3.Row, dep_card_id: int) -> None:
    """前置卡片回执变化后，刷新依赖它的未揭示卡片的门槛状态（仅主持人可见）。

    已揭示卡片的门槛状态（unlocked/timeout）是历史事实，不再随回执变化。
    须在 state_lock 下调用。
    """
    dependents = db.execute(
        "SELECT * FROM cards WHERE depends_on_card_id = ? AND revealed = 0", (dep_card_id,)
    ).fetchall()
    for card in dependents:
        await publish(room["code"], room["id"], "gate_updated",
                      {"card_id": card["id"], "gate": gate_view(card)})


async def publish_room_gate_refresh(room: sqlite3.Row) -> None:
    """参与组增删 / 重置后，重算全房间未揭示卡片的门槛状态。须在 state_lock 下调用。"""
    gated = db.execute(
        """SELECT * FROM cards
           WHERE room_id = ? AND depends_on_card_id IS NOT NULL AND revealed = 0""",
        (room["id"],),
    ).fetchall()
    for card in gated:
        await publish(room["code"], room["id"], "gate_updated",
                      {"card_id": card["id"], "gate": gate_view(card)})


# ---------------------------------------------------------------- 观察席分享

# 视野（scope）三种取值：None = 主持人（全量）；int = 参与组 id（玩家）；
# ObserverScope = 观察者（只读、仅已揭示、限定所选参与组）。
class ObserverScope(NamedTuple):
    token: str                     # 观察凭证（撤销/到期时按它断开连接）
    group_ids: frozenset[int]      # 主持人选定的 1~3 个参与组


def share_active(share: sqlite3.Row) -> bool:
    """凭证当前是否有效：未撤销且未到期。"""
    return not share["revoked"] and share["expires_at"] > time.time()


def observer_scope_from(share: sqlite3.Row) -> ObserverScope:
    return ObserverScope(token=share["token"],
                         group_ids=frozenset(json.loads(share["group_ids"])))


def get_active_share_or_410(token: str) -> sqlite3.Row:
    """解析观察凭证：不存在 404；已撤销/已到期 410（刷新重连均不可恢复）。"""
    share = db.execute(
        "SELECT * FROM observer_shares WHERE token = ?", (token,)
    ).fetchone()
    if share is None:
        raise HTTPException(status_code=404, detail="share not found")
    if not share_active(share):
        raise HTTPException(status_code=410, detail="share expired or revoked")
    return share


def share_public(share: sqlite3.Row) -> dict:
    """主持人视角的分享序列化。含观察凭证，只随主持人快照/事件下发，绝不给玩家与观察者。"""
    gids: list[int] = json.loads(share["group_ids"])
    names: dict[int, str] = {}
    if gids:
        marks = ",".join("?" * len(gids))
        for row in db.execute(f"SELECT id, name FROM groups WHERE id IN ({marks})", gids):
            names[row["id"]] = row["name"]
    revoked = bool(share["revoked"])
    expired = share["expires_at"] <= time.time()
    return {
        "token": share["token"],
        "group_ids": gids,
        "group_names": [names.get(gid, f"#{gid}") for gid in gids],
        "minutes": max(1, round((share["expires_at"] - share["created_at"]) / 60)),
        "created_at": share["created_at"],
        "expires_at": share["expires_at"],
        "status": "revoked" if revoked else ("expired" if expired else "active"),
    }


def build_snapshot(room: sqlite3.Row, scope: int | None | ObserverScope) -> dict:
    """权威全量状态，按视野过滤：主持人拿全部；参与者只拿全员卡片与本组卡片；
    观察者只拿已揭示的全员卡片与所选组卡片（只读，无回执数据）。

    回执数据同样按视野下发：主持人拿到每张卡的 expected/acked 汇总，
    参与者只拿到本组是否已回执（my_receipt），观察者完全拿不到回执。
    处置答复同理：主持人拿到每张卡的 expected/submitted（含各组正文与版本），
    参与者只拿到本组答复（my_answer），观察者完全拿不到答复。
    """
    if isinstance(scope, ObserverScope):
        gids = sorted(scope.group_ids)
        marks = ",".join("?" * len(gids))
        cards = db.execute(
            f"""SELECT * FROM cards
                WHERE room_id = ? AND revealed = 1
                  AND (group_id IS NULL OR group_id IN ({marks}))
                ORDER BY trigger_seconds, seq""",
            (room["id"], *gids),
        ).fetchall()
        groups = db.execute(
            f"SELECT * FROM groups WHERE id IN ({marks}) ORDER BY id", gids
        ).fetchall()
        last_id = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
        ).fetchone()["m"]
        return {
            "kind": "snapshot",
            "last_event_id": last_id,
            "server_now_ms": now_ms(),
            "round": current_round(room["id"]),  # 当前轮次（房间级，非敏感）
            "scope": {"role": "observer", "group_ids": gids},
            "room": room_public(room, include_code=False),       # 观察者拿不到房间码
            "groups": [{"id": g["id"], "name": g["name"]} for g in groups],  # 不含组加入码
            "cards": [card_public(c) for c in cards],            # 仅已揭示；无回执字段
        }
    if scope is None:
        cards = db.execute(
            "SELECT * FROM cards WHERE room_id = ? ORDER BY trigger_seconds, seq",
            (room["id"],),
        ).fetchall()
        groups = db.execute(
            "SELECT * FROM groups WHERE room_id = ? ORDER BY id", (room["id"],)
        ).fetchall()
    else:
        cards = db.execute(
            """SELECT * FROM cards
               WHERE room_id = ? AND (group_id IS NULL OR group_id = ?)
               ORDER BY trigger_seconds, seq""",
            (room["id"], scope),
        ).fetchall()
        groups = db.execute("SELECT * FROM groups WHERE id = ?", (scope,)).fetchall()
    last_id = db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
    ).fetchone()["m"]
    snapshot = {
        "kind": "snapshot",
        "last_event_id": last_id,
        "server_now_ms": now_ms(),
        "round": current_round(room["id"]),  # 当前轮次（= 此前 reset 事件数）
        "scope": {"role": "host" if scope is None else "player", "group_id": scope},
        "room": room_public(room, include_code=scope is None),
        "groups": [group_public(g) for g in groups],
        "cards": snapshot_cards(room, cards, groups, scope),
    }
    if scope is None:
        # 主持人快照附带全部观察分享（有效/失效），供控制台列出与管理
        shares = db.execute(
            "SELECT * FROM observer_shares WHERE room_id = ? ORDER BY id DESC", (room["id"],)
        ).fetchall()
        snapshot["shares"] = [share_public(s) for s in shares]
        # 复盘锚点仅主持人可见：按 (轮次, 虚拟毫秒, id) 列出全部历史轮次的锚点。
        # 重置不清锚点，旧轮次锚点仍归属原轮次保留。
        anchors = db.execute(
            "SELECT * FROM anchors WHERE room_id = ? ORDER BY round, vtime_ms, id",
            (room["id"],),
        ).fetchall()
        snapshot["anchors"] = [anchor_public(a) for a in anchors]
        # 现场决策投票：主持人看到全部轮次的投票（作废/已结算含历史结果）
        polls = db.execute(
            "SELECT * FROM polls WHERE room_id = ? ORDER BY round, id", (room["id"],)
        ).fetchall()
        snapshot["polls"] = [poll_host_view(p) for p in polls]
        # 应急车辆申请：主持人看到车辆池总量/余量与各组申请、队列（未建池为 None）
        snapshot["vehicles"] = vehiclemod.host_view(db, room["id"])
        # 现场情报核验板：主持人看到全部命题的逐组报告与汇总状态
        claims = db.execute(
            "SELECT * FROM intel_claims WHERE room_id = ? ORDER BY id", (room["id"],)
        ).fetchall()
        snapshot["claims"] = [intel_host_view(c) for c in claims]
    elif scope is not None:
        # 参与者：只拿当前轮次、白名单包含本组的投票；
        # 已结算投票也由此下发（结算后才可见结果），旧轮次投票一律不可见
        round_idx = current_round(room["id"])
        polls = db.execute(
            """SELECT p.* FROM polls p
               JOIN poll_groups pg ON pg.poll_id = p.id
               WHERE p.room_id = ? AND p.round = ? AND pg.group_id = ?
               ORDER BY p.id""",
            (room["id"], round_idx, scope),
        ).fetchall()
        snapshot["polls"] = [poll_player_view(p, scope) for p in polls]
        # 应急车辆申请：参与组只看到本组申请状态与排位（看不到池余量与他组申请）
        snapshot["vehicles"] = {"my_request": vehiclemod.my_view(db, room["id"], scope)}
        # 现场情报核验板：参与组只看到白名单含本组的命题，
        # 且只见本组报告与汇总状态（看不到他组立场明细与依据正文）
        claims = db.execute(
            """SELECT c.* FROM intel_claims c
               JOIN intel_claim_groups cg ON cg.claim_id = c.id
               WHERE c.room_id = ? AND cg.group_id = ?
               ORDER BY c.id""",
            (room["id"], scope),
        ).fetchall()
        snapshot["claims"] = [intel_player_view(c, scope) for c in claims]
    return snapshot


def snapshot_cards(room: sqlite3.Row, cards: list[sqlite3.Row],
                   groups: list[sqlite3.Row], scope: int | None) -> list[dict]:
    """为快照中的卡片附加回执与答复视图（按视野分别计算）。"""
    if scope is None:
        # 主持人：每张卡 expected = 全员卡取全部参与组 / 定向卡取目标组；acked = 已回执组
        acked_map: dict[int, set[int]] = {}
        for row in db.execute(
            "SELECT card_id, group_id FROM receipts WHERE room_id = ?", (room["id"],)
        ):
            acked_map.setdefault(row["card_id"], set()).add(row["group_id"])
        # 处置答复：按卡片聚合各组已提交的正文与版本（与回执同一 expected 口径）
        answers_map: dict[int, dict[int, sqlite3.Row]] = {}
        for row in db.execute("SELECT * FROM answers WHERE room_id = ?", (room["id"],)):
            answers_map.setdefault(row["card_id"], {})[row["group_id"]] = row
        group_ids = [g["id"] for g in groups]
        out = []
        for card in cards:
            data = card_public(card)
            expected = group_ids if card["group_id"] is None else [card["group_id"]]
            acked = acked_map.get(card["id"], set())
            data["receipts"] = {
                "expected": expected,
                "acked": [gid for gid in expected if gid in acked],
            }
            submitted = answers_map.get(card["id"], {})
            data["answers"] = {
                "expected": expected,
                "submitted": [answer_public(submitted[gid])
                              for gid in expected if gid in submitted],
            }
            gate = gate_view(card)  # 门槛状态仅主持人可见
            if gate is not None:
                data["gate"] = gate
            out.append(data)
        return out
    # 参与者：仅本组是否已回执、本组答复（看不到他组正文）
    mine = {
        row["card_id"]
        for row in db.execute(
            "SELECT card_id FROM receipts WHERE room_id = ? AND group_id = ?",
            (room["id"], scope),
        )
    }
    my_answers = {
        row["card_id"]: row
        for row in db.execute(
            "SELECT * FROM answers WHERE room_id = ? AND group_id = ?",
            (room["id"], scope),
        )
    }
    out = []
    for card in cards:
        data = card_public(card)
        data["my_receipt"] = card["id"] in mine
        ans = my_answers.get(card["id"])
        data["my_answer"] = (
            {"body": ans["body"], "revision": ans["revision"]} if ans is not None else None
        )
        out.append(data)
    return out


# ---------------------------------------------------------------- 广播

def poll_event_group_ids(payload: dict) -> set[int]:
    """poll_created / poll_settled 载荷里的参与组白名单（用于按视野过滤）。"""
    return set(payload.get("group_ids") or [])


def event_visible(scope: int | None | ObserverScope, type_: str, payload: dict) -> bool:
    """事件是否应投递给该视野。主持人全收；参与者只收全员/本组卡片事件与房间级事件；
    观察者只收已揭示卡片揭示/移除与房间级事件。"""
    if scope is None:
        return True
    if type_.startswith("share_") or type_ == "gate_updated" or type_.startswith("anchor_") \
            or type_ in ("poll_voided", "poll_settled") or type_ == "vehicle_pool_created":
        # 分享凭证、门槛编排、复盘锚点、投票作废、车辆池创建仅主持人可见；
        # 投票结算对参与者的推送走 Hub.send_poll_event（按组脱敏载荷），
        # 通用广播只给主持人，避免把逐组选票全量载荷发给参与者。
        return False
    if isinstance(scope, ObserverScope):
        if type_ in ("card_revealed", "card_removed", "answer_lock_updated"):
            gid = payload.get("group_id")
            return gid is None or gid in scope.group_ids
        if type_ in ("group_renamed", "group_removed"):
            return payload.get("id") in scope.group_ids
        if type_ in ("card_added", "group_added", "receipt_updated", "answer_updated",
                     "poll_created", "poll_vote_updated", "poll_settled",
                     "vehicle_request_submitted", "vehicle_request_cancelled") \
                or type_.startswith("intel_"):
            return False  # 观察者完全不可见：未揭示卡、组加入码、回执、答复、投票、车辆申请、情报核验
        return True  # clock / reset 等房间级事件
    if type_ in ("card_added", "card_revealed", "card_removed", "answer_lock_updated"):
        gid = payload.get("group_id")
        return gid is None or gid == scope
    if type_ in ("receipt_updated", "answer_updated"):
        return payload.get("group_id") == scope  # 参与者只感知本组的回执/答复变化
    if type_ == "poll_vote_updated":
        return payload.get("group_id") == scope  # 改投回执仅同组同步（主持人全收）
    if type_ in ("poll_created", "poll_settled"):
        return scope in poll_event_group_ids(payload)  # 仅白名单参与组可见
    if type_ == "vehicle_request_submitted":
        return payload.get("group_id") == scope  # 仅提交组收本组脱敏载荷（主持人全收）
    if type_ == "vehicle_request_cancelled":
        # 受影响组（被取消组 + 补位/排位变化组）收各自脱敏载荷，其余组不感知
        return scope in (payload.get("affected_group_ids") or [])
    if type_.startswith("intel_"):
        # 情报核验：仅命题白名单内的参与组可见（收各自脱敏载荷，主持人全收）
        return scope in (payload.get("group_ids") or [])
    if type_ == "group_renamed":
        return payload.get("id") == scope  # 参与者只需感知本组改名
    if type_ in ("group_added", "group_removed"):
        return False
    return True  # clock / reset 等房间级事件


class Hub:
    """按房间维护连接及其视野（None=主持人，int=参与组，ObserverScope=观察者），
    广播时按视野过滤。"""

    def __init__(self) -> None:
        self._rooms: dict[str, dict[WebSocket, int | None | ObserverScope]] = {}

    def add(self, code: str, ws: WebSocket, scope: int | None | ObserverScope) -> None:
        self._rooms.setdefault(code, {})[ws] = scope

    def discard(self, code: str, ws: WebSocket) -> None:
        subs = self._rooms.get(code)
        if subs is not None:
            subs.pop(ws, None)
            if not subs:
                self._rooms.pop(code, None)

    async def broadcast(self, code: str, message: dict) -> None:
        type_ = message.get("type", "")
        payload = message.get("payload") or {}
        # 部分事件（投票创建/结算）落库的是主持人全量载荷，参与者需换成
        # 不含他组信息的脱敏载荷：以下两个私有字段只在本进程内路由，绝不外发。
        player_payload = message.get("_player_payload")          # 所有参与者通用
        player_payloads = message.get("_player_payloads") or {}  # 按参与组 id 区分
        out_base = {k: v for k, v in message.items()
                    if k not in ("_player_payload", "_player_payloads")}
        for ws, scope in list(self._rooms.get(code, {}).items()):
            if not event_visible(scope, type_, payload):
                continue
            try:
                if isinstance(scope, int) and scope in player_payloads:
                    await ws.send_json({**out_base, "payload": player_payloads[scope]})
                elif isinstance(scope, int) and player_payload is not None:
                    await ws.send_json({**out_base, "payload": player_payload})
                else:
                    await ws.send_json(out_base)
            except Exception:
                self.discard(code, ws)

    async def close_scope(self, code: str, scope: int, message: dict) -> None:
        """组被删除后，主动断开该组所有参与者连接。"""
        for ws, ws_scope in list(self._rooms.get(code, {}).items()):
            if ws_scope != scope:
                continue
            try:
                await ws.send_json(message)
                await ws.close(code=4404)
            except Exception:
                pass
            self.discard(code, ws)

    async def close_observer(self, code: str, token: str, message: dict) -> None:
        """分享撤销/到期后，主动断开该观察凭证的所有连接。"""
        for ws, ws_scope in list(self._rooms.get(code, {}).items()):
            if not (isinstance(ws_scope, ObserverScope) and ws_scope.token == token):
                continue
            try:
                await ws.send_json(message)
                await ws.close(code=4403)
            except Exception:
                pass
            self.discard(code, ws)

    async def send_poll_event(self, code: str, poll: sqlite3.Row,
                              event_id: int, type_: str) -> None:
        """向该投票白名单内在线的参与组连接定向投递其脱敏载荷。

        用于结算事件：主持人已由 publish 收到全量载荷，参与者需收到
        poll_player_view（本组已投选项 + 获胜结果，不含逐组选票）。
        """
        gids = pollmod.poll_group_ids(db, poll["id"])
        for ws, scope in list(self._rooms.get(code, {}).items()):
            if not isinstance(scope, int) or scope not in gids:
                continue
            try:
                await ws.send_json({
                    "kind": "event",
                    "id": event_id,
                    "type": type_,
                    "payload": poll_player_view(poll, scope),
                    "server_now_ms": now_ms(),
                })
            except Exception:
                self.discard(code, ws)


hub = Hub()


async def publish(code: str, room_id: int, type_: str, payload: dict,
                  *, player_payload: dict | None = None,
                  player_payloads: dict[int, dict] | None = None) -> int:
    """落库一条递增事件并按视野广播。必须在 state_lock 下调用。返回新事件 id。

    落库与主持人收到的始终是 payload；提供 player_payload 时，参与者连接
    收到脱敏后的替代载荷（如投票事件：主持人看逐组选票，参与者只看自己的
    选择/结算结果）；提供 player_payloads 时按参与组 id 分别投递不同脱敏载荷
    （如车辆申请：每组只收本组状态），事件 id 不变，重连仍以快照为准。
    """
    cur = db.execute(
        "INSERT INTO events (room_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), time.time()),
    )
    db.commit()
    await hub.broadcast(code, {
        "kind": "event",
        "id": cur.lastrowid,
        "type": type_,
        "payload": payload,
        "server_now_ms": now_ms(),
        **({"_player_payload": player_payload} if player_payload is not None else {}),
        **({"_player_payloads": player_payloads} if player_payloads is not None else {}),
    })
    return cur.lastrowid


# ---------------------------------------------------------------- 时钟 tick

async def expire_due_shares() -> None:
    """把刚到期的观察分享标记失效：广播 share_expired 给主持人、断开在线观察者。

    须在 state_lock 下调用。expired_notified 持久化，保证重启后不会重复广播；
    到期判定以 expires_at 为准，凭证此后 REST/WS 一律 410，刷新重连不可恢复。
    """
    due = db.execute(
        """SELECT s.id, s.room_id, s.token, r.code AS room_code
           FROM observer_shares s JOIN rooms r ON r.id = s.room_id
           WHERE s.expires_at <= ? AND s.revoked = 0 AND s.expired_notified = 0""",
        (time.time(),),
    ).fetchall()
    for share in due:
        db.execute("UPDATE observer_shares SET expired_notified = 1 WHERE id = ?", (share["id"],))
        db.commit()
        await publish(share["room_code"], share["room_id"], "share_expired",
                      {"token": share["token"]})
        await hub.close_observer(share["room_code"], share["token"],
                                 {"kind": "error", "message": "观察链接已到期"})
        log.info("share %s expired, observers disconnected", share["token"][:8])


async def ticker_loop() -> None:
    """周期性扫描 running 房间，按 (trigger_seconds, seq) 稳定揭示到期卡片。

    带回执门槛的卡片到期后先检查前置卡片回执：未集齐则继续等待；
    虚拟时钟超过 触发秒数 + 超时秒数 仍未集齐时，标记为超时揭示并继续。
    暂停期间时钟冻结，门槛计时同样不推进。
    """
    while True:
        await asyncio.sleep(TICK_INTERVAL)
        try:
            async with state_lock:
                running = db.execute("SELECT * FROM rooms WHERE status = 'running'").fetchall()
                for room in running:
                    elapsed = room_elapsed_ms(room)
                    due = db.execute(
                        """SELECT * FROM cards
                           WHERE room_id = ? AND revealed = 0 AND trigger_seconds * 1000 <= ?
                           ORDER BY trigger_seconds, seq""",
                        (room["id"], elapsed),
                    ).fetchall()
                    for card in due:
                        timed_out = False
                        if card["depends_on_card_id"] is not None:
                            dep = db.execute(
                                "SELECT * FROM cards WHERE id = ?",
                                (card["depends_on_card_id"],),
                            ).fetchone()
                            if gate_pending_groups(card, dep):
                                timeout = card["gate_timeout_seconds"]
                                deadline = (card["trigger_seconds"] + timeout) * 1000 if timeout is not None else None
                                if deadline is None or elapsed < deadline:
                                    continue  # 门槛未满足且未超时：保持等待
                                timed_out = True  # 虚拟时钟超时：标记为超时揭示并继续
                        db.execute(
                            "UPDATE cards SET revealed = 1, revealed_at_ms = ?, timed_out = ? WHERE id = ?",
                            (elapsed, 1 if timed_out else 0, card["id"]),
                        )
                        db.commit()
                        fresh = db.execute("SELECT * FROM cards WHERE id = ?", (card["id"],)).fetchone()
                        await publish(room["code"], room["id"], "card_revealed", card_public(fresh))
                        if timed_out:
                            await publish(room["code"], room["id"], "gate_updated",
                                          {"card_id": fresh["id"], "gate": gate_view(fresh)})
                            log.info("room %s gate timeout, revealed card #%s (%s)",
                                     room["code"], card["seq"], card["title"])
                        else:
                            log.info("room %s revealed card #%s (%s)", room["code"], card["seq"], card["title"])
                    # 现场决策投票：对本轮到期投票在同一把锁内一次性结算。
                    # 暂停时房间不在 running 列表里，截止计时随之冻结；
                    # settle_due_polls 只取 status='open' 行并写回 settled，
                    # 每条投票只结算一次，重启后不会重复结算/广播。
                    round_idx = current_round(room["id"])
                    for settled in pollmod.settle_due_polls(db, room["id"], round_idx, elapsed):
                        host_payload = poll_host_view(settled.poll)
                        event_id = await publish(
                            room["code"], room["id"], "poll_settled", host_payload)
                        # 参与者收到的是按组构造的脱敏载荷（本组已投选项 +
                        # 获胜结果/汇总，不含逐组选票）；主持人已在上一步收到全量。
                        await hub.send_poll_event(
                            room["code"], settled.poll, event_id, "poll_settled")
                        log.info("room %s poll %s settled, winner_option=%s",
                                 room["code"], settled.poll["id"],
                                 host_payload["winner_option_id"])
                await expire_due_shares()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ticker error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    db = dbmod.connect()
    dbmod.init_schema(db)  # 启动即完成数据库初始化
    task = asyncio.create_task(ticker_loop())
    log.info("inject console ready, db=%s", dbmod.DB_PATH)
    yield
    task.cancel()
    db.close()


app = FastAPI(title="情景注入台", lifespan=lifespan)


# ---------------------------------------------------------------- REST：房间

class RoomIn(BaseModel):
    name: str = Field(default="", max_length=120)


@app.post("/api/rooms", status_code=201)
async def create_room(body: RoomIn):
    async with state_lock:
        code = fresh_code()
        db.execute(
            "INSERT INTO rooms (code, name, created_at) VALUES (?, ?, ?)",
            (code, body.name.strip(), time.time()),
        )
        db.commit()
        return {"code": code, "name": body.name.strip()}


@app.get("/api/rooms/{code}")
async def get_room(code: str):
    """主持人快照：含全部卡片与全部分组（房间码即主持人凭证）。"""
    async with state_lock:
        room = get_room_or_404(code)
        return build_snapshot(room, None)


@app.get("/api/join/{join_code}")
async def join_room(join_code: str):
    """参与者入口：凭组加入码换取按该组过滤的快照（不含房间码与他组卡片）。"""
    async with state_lock:
        group = db.execute(
            "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
        ).fetchone()
        if group is None:
            raise HTTPException(status_code=404, detail="join code not found")
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        return build_snapshot(room, group["id"])


# ---------------------------------------------------------------- REST：参与组

class GroupIn(BaseModel):
    name: str = Field(min_length=1, max_length=60)


@app.post("/api/rooms/{code}/groups", status_code=201)
async def add_group(code: str, body: GroupIn):
    async with state_lock:
        room = get_room_or_404(code)
        count = db.execute(
            "SELECT COUNT(*) AS c FROM groups WHERE room_id = ?", (room["id"],)
        ).fetchone()["c"]
        if count >= MAX_GROUPS:
            raise HTTPException(status_code=409, detail=f"at most {MAX_GROUPS} groups per room")
        cur = db.execute(
            "INSERT INTO groups (room_id, name, join_code, created_at) VALUES (?, ?, ?, ?)",
            (room["id"], body.name.strip(), fresh_code(), time.time()),
        )
        db.commit()
        group = db.execute("SELECT * FROM groups WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = group_public(group)
        await publish(room["code"], room["id"], "group_added", payload)
        # 新组成为所有全员卡的待回执方，相关门槛可能由解锁回到等待
        await publish_room_gate_refresh(room)
        return payload


@app.patch("/api/rooms/{code}/groups/{group_id}")
async def rename_group(code: str, group_id: int, body: GroupIn):
    async with state_lock:
        room = get_room_or_404(code)
        get_group_or_404(room["id"], group_id)
        name = body.name.strip()
        db.execute("UPDATE groups SET name = ? WHERE id = ?", (name, group_id))
        db.commit()
        payload = {"id": group_id, "name": name}
        await publish(room["code"], room["id"], "group_renamed", payload)
        return payload


@app.delete("/api/rooms/{code}/groups/{group_id}")
async def delete_group(code: str, group_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        get_group_or_404(room["id"], group_id)
        refs = db.execute(
            "SELECT COUNT(*) AS c FROM cards WHERE group_id = ?", (group_id,)
        ).fetchone()["c"]
        if refs:
            raise HTTPException(status_code=409, detail=f"group is targeted by {refs} card(s)")
        # 该组若持有车辆申请，先记下（行随组级联删除）；已分配申请释放车辆后需补位
        had_vehicle_request = db.execute(
            "SELECT 1 FROM vehicle_requests WHERE group_id = ?", (group_id,)
        ).fetchone() is not None
        # 该组在哪些情报核验命题的白名单内（其报告随组级联删除后，这些命题需即时重算）
        affected_claim_ids = [r["claim_id"] for r in db.execute(
            "SELECT claim_id FROM intel_claim_groups WHERE group_id = ?", (group_id,)
        )]
        db.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        db.commit()
        await publish(room["code"], room["id"], "group_removed", {"id": group_id})
        # 该组回执已级联删除、全员卡期望组收缩，相关门槛状态随之变化
        await publish_room_gate_refresh(room)
        # 应急车辆：该组申请已随组级联删除；若释放了已分配车辆（或排队排位
        # 前移），从队首补位并同步仍持有申请的各组（队首数量不足不跳过）
        if vehiclemod.get_pool(db, room["id"]) is not None:
            newly = vehiclemod.fill_queue(db, room["id"])
            if had_vehicle_request or newly:
                player_payloads = _vehicle_player_payloads(room["id"], (group_id,))
                await publish(
                    room["code"], room["id"], "vehicle_request_cancelled",
                    {"group_id": group_id,
                     "affected_group_ids": sorted(player_payloads),
                     "allocated_group_ids": [r["group_id"] for r in newly],
                     "vehicles": vehiclemod.host_view(db, room["id"])},
                    player_payloads=player_payloads,
                )
        # 加入码即刻失效，踢下仍在连接的该组参与者
        await hub.close_scope(room["code"], group_id, {"kind": "error", "message": "所在参与组已被主持人删除"})
        # 情报核验：该组报告已随组级联删除、白名单收缩，相关命题状态即时重算，
        # 同步给主持人与其余白名单组（被删组连接已断开，不在分发之列）
        for claim_id in affected_claim_ids:
            await publish_intel_claim(room, claim_id, "intel_claim_updated")
        return {"ok": True}


# ---------------------------------------------------------------- REST：卡片

class CardIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(default="", max_length=4000)
    trigger_seconds: int = Field(ge=0, le=24 * 3600)
    group_id: int | None = None  # None = 全员，否则仅该参与组可见
    depends_on_card_id: int | None = None  # 回执门槛：前置卡片 id；None = 无门槛
    gate_timeout_seconds: int | None = Field(default=None, ge=1, le=24 * 3600)  # 虚拟时钟超时秒数


@app.post("/api/rooms/{code}/cards", status_code=201)
async def add_card(code: str, body: CardIn):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        if body.group_id is not None:
            get_group_or_404(room["id"], body.group_id)  # 目标组必须属于本房间
        if body.depends_on_card_id is not None:
            dep = db.execute(
                "SELECT 1 FROM cards WHERE id = ? AND room_id = ?",
                (body.depends_on_card_id, room["id"]),
            ).fetchone()
            if dep is None:
                raise HTTPException(status_code=404, detail="dependency card not found")
            # 依赖边只会指向更早创建的卡片，依赖图天然是无环的，无需环检测
        elif body.gate_timeout_seconds is not None:
            raise HTTPException(status_code=400,
                                detail="gate_timeout_seconds requires depends_on_card_id")
        seq = db.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM cards WHERE room_id = ?", (room["id"],)
        ).fetchone()["s"]
        cur = db.execute(
            "INSERT INTO cards (room_id, seq, trigger_seconds, title, content, group_id,"
            " depends_on_card_id, gate_timeout_seconds)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (room["id"], seq, body.trigger_seconds, body.title.strip(), body.content,
             body.group_id, body.depends_on_card_id, body.gate_timeout_seconds),
        )
        db.commit()
        card = db.execute("SELECT * FROM cards WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = card_public(card)
        await publish(room["code"], room["id"], "card_added", payload)
        if card["depends_on_card_id"] is not None:
            # card_added 不带门槛字段，单独下发主持人专属的门槛视图
            await publish(room["code"], room["id"], "gate_updated",
                          {"card_id": card["id"], "gate": gate_view(card)})
        return payload


@app.delete("/api/rooms/{code}/cards/{card_id}")
async def delete_card(code: str, card_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        card = db.execute(
            "SELECT * FROM cards WHERE id = ? AND room_id = ?", (card_id, room["id"])
        ).fetchone()
        if card is None:
            raise HTTPException(status_code=404, detail="card not found")
        deps = db.execute(
            "SELECT COUNT(*) AS c FROM cards WHERE depends_on_card_id = ?", (card_id,)
        ).fetchone()["c"]
        if deps:
            raise HTTPException(status_code=409,
                                detail=f"card is a gate dependency of {deps} card(s)")
        db.execute("DELETE FROM cards WHERE id = ?", (card_id,))
        db.commit()
        # 带上 group_id，广播过滤才能判断该事件是否属于某参与组的视野
        await publish(room["code"], room["id"], "card_removed",
                      {"id": card_id, "group_id": card["group_id"]})
        return {"ok": True}


# ---------------------------------------------------------------- REST：回执

def resolve_group_card(join_code: str, card_id: int) -> tuple[sqlite3.Row, sqlite3.Row]:
    """把参与者请求（回执/答复）解析为 (参与组, 卡片)。

    参与者凭组加入码操作，且只能操作本组可见的卡片（全员卡或目标为本组的卡）；
    不可见的卡片一律 404，不向参与者暴露其存在。
    """
    group = db.execute(
        "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
    ).fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="join code not found")
    card = db.execute(
        """SELECT * FROM cards
           WHERE id = ? AND room_id = ? AND (group_id IS NULL OR group_id = ?)""",
        (card_id, group["room_id"], group["id"]),
    ).fetchone()
    if card is None:
        raise HTTPException(status_code=404, detail="card not found")
    return group, card


@app.post("/api/join/{join_code}/cards/{card_id}/receipt")
async def ack_card(join_code: str, card_id: int):
    """参与者标记「已收到」。幂等：同一参与组对同一卡片仅保留一条回执。"""
    async with state_lock:
        group, card = resolve_group_card(join_code, card_id)
        if not card["revealed"]:
            raise HTTPException(status_code=409, detail="card not revealed yet")
        existing = db.execute(
            "SELECT 1 FROM receipts WHERE card_id = ? AND group_id = ?",
            (card_id, group["id"]),
        ).fetchone()
        if existing is None:
            db.execute(
                "INSERT INTO receipts (room_id, card_id, group_id, created_at)"
                " VALUES (?, ?, ?, ?)",
                (group["room_id"], card_id, group["id"], time.time()),
            )
            db.commit()
            room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
            await publish(room["code"], room["id"], "receipt_updated",
                          {"card_id": card_id, "group_id": group["id"], "acked": True})
            await publish_gate_refresh(room, card_id)  # 可能解锁依赖本卡的门槛
        return {"card_id": card_id, "group_id": group["id"], "acked": True}


@app.delete("/api/join/{join_code}/cards/{card_id}/receipt")
async def unack_card(join_code: str, card_id: int):
    """参与者撤销回执。幂等：本就没有回执时直接返回当前状态。"""
    async with state_lock:
        group, card = resolve_group_card(join_code, card_id)
        cur = db.execute(
            "DELETE FROM receipts WHERE card_id = ? AND group_id = ?",
            (card_id, group["id"]),
        )
        db.commit()
        if cur.rowcount:
            room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
            await publish(room["code"], room["id"], "receipt_updated",
                          {"card_id": card_id, "group_id": group["id"], "acked": False})
            await publish_gate_refresh(room, card_id)  # 依赖本卡的门槛可能回到等待
        return {"card_id": card_id, "group_id": group["id"], "acked": False}


# ---------------------------------------------------------------- REST：处置答复

class AnswerIn(BaseModel):
    body: str = Field(default="", max_length=4000)
    revision: int = Field(ge=0)  # 乐观并发：须等于服务端当前版本；尚无答复时传 0


@app.put("/api/join/{join_code}/cards/{card_id}/answer")
async def put_answer(join_code: str, card_id: int, body: AnswerIn):
    """参与者提交/修改本组答复：每张已揭示卡片每组一份，同组多人共享。

    乐观并发：revision 必须等于服务端当前版本（尚无答复时传 0），版本过期
    返回 409 并在 detail.answer 中携带最新答复，防止同组多窗口静默覆盖；
    主持人锁定后参与者只读，写入返回 409（detail.error = answers_locked）。
    """
    async with state_lock:
        group, card = resolve_group_card(join_code, card_id)
        if not card["revealed"]:
            raise HTTPException(status_code=409, detail="card not revealed yet")
        if card["answers_locked"]:
            raise HTTPException(status_code=409, detail={"error": "answers_locked"})
        existing = db.execute(
            "SELECT * FROM answers WHERE card_id = ? AND group_id = ?",
            (card_id, group["id"]),
        ).fetchone()
        current = existing["revision"] if existing is not None else 0
        if body.revision != current:
            raise HTTPException(status_code=409, detail={
                "error": "revision_conflict",
                "answer": answer_public(existing) if existing is not None else None,
            })
        now = time.time()
        if existing is None:
            db.execute(
                "INSERT INTO answers (room_id, card_id, group_id, body, revision, updated_at)"
                " VALUES (?, ?, ?, ?, 1, ?)",
                (group["room_id"], card_id, group["id"], body.body, now),
            )
        else:
            db.execute(
                "UPDATE answers SET body = ?, revision = revision + 1, updated_at = ?"
                " WHERE id = ?",
                (body.body, now, existing["id"]),
            )
        db.commit()
        fresh = db.execute(
            "SELECT * FROM answers WHERE card_id = ? AND group_id = ?",
            (card_id, group["id"]),
        ).fetchone()
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        payload = answer_public(fresh)
        # 含答复正文：仅主持人与本组参与者可收到（event_visible 按组过滤）
        await publish(room["code"], room["id"], "answer_updated", payload)
        return payload


async def _set_answers_locked(code: str, card_id: int, locked: bool):
    """逐卡锁定/重新开放处置答复。幂等：已处于目标状态时直接返回，不发事件。"""
    async with state_lock:
        room = get_room_or_404(code)
        card = db.execute(
            "SELECT * FROM cards WHERE id = ? AND room_id = ?", (card_id, room["id"])
        ).fetchone()
        if card is None:
            raise HTTPException(status_code=404, detail="card not found")
        if bool(card["answers_locked"]) != locked:
            db.execute(
                "UPDATE cards SET answers_locked = ? WHERE id = ?",
                (1 if locked else 0, card_id),
            )
            db.commit()
            # 带上卡片的 group_id，广播过滤才能判断该事件属于哪些参与组的视野
            await publish(room["code"], room["id"], "answer_lock_updated",
                          {"card_id": card_id, "group_id": card["group_id"],
                           "answers_locked": locked})
        return {"card_id": card_id, "answers_locked": locked}


@app.post("/api/rooms/{code}/cards/{card_id}/answers/lock")
async def lock_answers(code: str, card_id: int):
    """主持人锁定该卡的处置答复：参与者只读，写入返回 409。"""
    return await _set_answers_locked(code, card_id, True)


@app.post("/api/rooms/{code}/cards/{card_id}/answers/unlock")
async def unlock_answers(code: str, card_id: int):
    """主持人重新开放该卡的处置答复。"""
    return await _set_answers_locked(code, card_id, False)


# ---------------------------------------------------------------- REST：观察席分享

class ShareIn(BaseModel):
    group_ids: list[int] = Field(min_length=1, max_length=3)  # 1～3 个参与组
    minutes: int = Field(ge=5, le=120)                        # 有效期 5～120 分钟


@app.post("/api/rooms/{code}/shares", status_code=201)
async def create_share(code: str, body: ShareIn):
    """主持人创建限时观察分享：随机凭证 + 到期时刻，落库持久化、可随时撤销。"""
    async with state_lock:
        room = get_room_or_404(code)
        group_ids = list(dict.fromkeys(body.group_ids))  # 去重保序
        if not group_ids:
            raise HTTPException(status_code=400, detail="select at least one group")
        for gid in group_ids:
            get_group_or_404(room["id"], gid)  # 每个目标组都必须属于本房间
        token = secrets.token_urlsafe(16)  # 128 位随机观察凭证，不可枚举
        db.execute(
            "INSERT INTO observer_shares (room_id, token, group_ids, expires_at, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (room["id"], token, json.dumps(group_ids), time.time() + body.minutes * 60, time.time()),
        )
        db.commit()
        share = db.execute("SELECT * FROM observer_shares WHERE token = ?", (token,)).fetchone()
        payload = share_public(share)
        await publish(room["code"], room["id"], "share_created", payload)
        return payload


@app.delete("/api/rooms/{code}/shares/{token}")
async def revoke_share(code: str, token: str):
    """撤销观察分享（幂等）：凭证即刻失效，该凭证的在线观察者立即被断开。"""
    async with state_lock:
        room = get_room_or_404(code)
        share = db.execute(
            "SELECT * FROM observer_shares WHERE token = ? AND room_id = ?",
            (token, room["id"]),
        ).fetchone()
        if share is None:
            raise HTTPException(status_code=404, detail="share not found")
        if not share["revoked"]:
            db.execute("UPDATE observer_shares SET revoked = 1 WHERE id = ?", (share["id"],))
            db.commit()
            await publish(room["code"], room["id"], "share_revoked", {"token": token})
        await hub.close_observer(room["code"], token,
                                 {"kind": "error", "message": "观察链接已被主持人撤销"})
        return {"ok": True, "token": token, "status": "revoked"}


@app.get("/api/observe/{token}")
async def observe_snapshot(token: str):
    """观察者入口：凭有效观察凭证换取只读快照。

    快照只含已揭示的全员卡片与所选组卡片；不含房间码、组加入码、
    未揭示卡片与回执明细。凭证撤销或到期后一律 410，刷新重连不可恢复。
    """
    async with state_lock:
        share = get_active_share_or_410(token)
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (share["room_id"],)).fetchone()
        return build_snapshot(room, observer_scope_from(share))


# ---------------------------------------------------------------- REST：复盘锚点

class AnchorIn(BaseModel):
    title: str = Field(min_length=1, max_length=120)
    note: str = Field(default="", max_length=4000)


class AnchorNoteIn(BaseModel):
    note: str = Field(default="", max_length=4000)  # 锚点只允许修改备注，标题与绑定不可变


def get_anchor_or_404(room_id: int, anchor_id: int) -> sqlite3.Row:
    anchor = db.execute(
        "SELECT * FROM anchors WHERE id = ? AND room_id = ?", (anchor_id, room_id)
    ).fetchone()
    if anchor is None:
        raise HTTPException(status_code=404, detail="anchor not found")
    return anchor


@app.post("/api/rooms/{code}/anchors", status_code=201)
async def create_anchor(code: str, body: AnchorIn):
    """主持人记录复盘锚点：任意房间状态（待机/运行/暂停）均可。

    绑定在同一把 state_lock 内原子完成，顺序严格为：先读当前轮次（此前 reset
    计数），再读当时事件水位（MAX(events.id)，锚点自身事件尚未落库），再读
    当前虚拟毫秒（running 结算当前运行段，paused/idle 取冻结值，暂停期间
    时间天然冻结），然后插入锚点行，落库并广播 anchor_created（其 id 恰为
    绑定水位 + 1），再把该事件 id 回填到行内 created_event_id。
    导出按 created_event_id <= 导出水位截断（锚点是否已存在），离线回放按
    event_id（绑定水位）截断；重置不删锚点，行内 round 使其归属原轮次。
    """
    async with state_lock:
        room = get_room_or_404(code)
        round_idx = current_round(room["id"])
        high_water = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
        ).fetchone()["m"]
        vtime_ms = room_elapsed_ms(room)
        now = time.time()
        # 先以占位 created_event_id = 绑定水位插入（同一锁内不会有读者看到中间态），
        # 随后落库 anchor_created 事件并把其自身事件 id 回填：导出按
        # created_event_id <= 导出水位截断，回放按 event_id（绑定水位）截断。
        cur = db.execute(
            "INSERT INTO anchors (room_id, title, note, round, vtime_ms, event_id,"
            " created_event_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (room["id"], body.title.strip(), body.note, round_idx, vtime_ms,
             high_water, high_water, now, now),
        )
        anchor_id = cur.lastrowid
        anchor = db.execute("SELECT * FROM anchors WHERE id = ?", (anchor_id,)).fetchone()
        payload = anchor_public(anchor)
        created_event_id = await publish(room["code"], room["id"], "anchor_created", payload)
        db.execute(
            "UPDATE anchors SET created_event_id = ? WHERE id = ?",
            (created_event_id, anchor_id),
        )
        db.commit()
        fresh = db.execute("SELECT * FROM anchors WHERE id = ?", (anchor_id,)).fetchone()
        return anchor_public(fresh)


@app.patch("/api/rooms/{code}/anchors/{anchor_id}")
async def update_anchor_note(code: str, anchor_id: int, body: AnchorNoteIn):
    """修改锚点备注（标题与创建时的轮次/虚拟毫秒/水位绑定不可变）。

    变更落库并以 anchor_updated 实时同步给主持人；参与者与观察者不可见。
    """
    async with state_lock:
        room = get_room_or_404(code)
        anchor = get_anchor_or_404(room["id"], anchor_id)
        now = time.time()
        db.execute(
            "UPDATE anchors SET note = ?, updated_at = ? WHERE id = ?",
            (body.note, now, anchor["id"]),
        )
        db.commit()
        fresh = db.execute("SELECT * FROM anchors WHERE id = ?", (anchor["id"],)).fetchone()
        payload = anchor_public(fresh)
        await publish(room["code"], room["id"], "anchor_updated", payload)
        return payload


@app.delete("/api/rooms/{code}/anchors/{anchor_id}")
async def delete_anchor(code: str, anchor_id: int):
    """删除锚点（主持人专属，幂等性通过 404 暴露不存在）。

    删除持久化并以 anchor_deleted 实时同步；重置演练不会删除任何锚点。
    """
    async with state_lock:
        room = get_room_or_404(code)
        anchor = get_anchor_or_404(room["id"], anchor_id)
        db.execute("DELETE FROM anchors WHERE id = ?", (anchor["id"],))
        db.commit()
        await publish(room["code"], room["id"], "anchor_deleted", {"id": anchor["id"]})
        return {"ok": True, "id": anchor["id"]}


# ---------------------------------------------------------------- REST：现场决策投票

class PollIn(BaseModel):
    question: str = Field(min_length=1, max_length=200)
    options: list[str] = Field(min_length=2, max_length=4)  # 2～4 个选项（创建顺序即平票决胜顺序）
    group_ids: list[int] = Field(min_length=1, max_length=MAX_GROUPS)  # 指定参与组（至少 1 个）
    deadline_seconds: int = Field(ge=1, le=24 * 3600)      # 截止虚拟秒（相对本轮开始，暂停冻结）


class PollVoteIn(BaseModel):
    option_id: int


@app.post("/api/rooms/{code}/polls", status_code=201)
async def create_poll(code: str, body: PollIn):
    """主持人创建现场决策投票：2～4 个选项、指定参与组、截止虚拟秒。

    任意房间状态（待机/运行/暂停）可创建；截止以虚拟时钟为准（相对本轮
    开始），故截止点必须晚于当前虚拟毫秒——已到期点无法投票，直接拒绝。
    投票归属当前轮次；tick 在状态锁内一次性结算并持久化。
    """
    async with state_lock:
        room = get_room_or_404(code)
        options = [s.strip()[:200] for s in body.options]
        if any(not s for s in options):
            raise HTTPException(status_code=400, detail="选项文本不能为空")
        if len(set(options)) != len(options):
            raise HTTPException(status_code=400, detail="选项文本不能重复")
        group_ids = list(dict.fromkeys(body.group_ids))  # 去重保序
        for gid in group_ids:
            get_group_or_404(room["id"], gid)  # 每个指定组都必须属于本房间
        deadline_ms = body.deadline_seconds * 1000
        elapsed = room_elapsed_ms(room)
        if deadline_ms <= elapsed:
            # 到点即到期：参与者没有任何可投票的虚拟时间窗口
            raise HTTPException(status_code=400,
                                detail="截止虚拟秒必须晚于当前虚拟时钟")
        round_idx = current_round(room["id"])
        poll = pollmod.create_poll(
            db, room["id"], body.question.strip(), options, group_ids,
            deadline_ms, round_idx,
        )
        host_payload = poll_host_view(poll)
        # 参与者通用脱敏载荷：open 状态下不含任何选票信息（每组只见自己的
        # 选择，由本组连接的快照/改投事件维护），广播按白名单过滤
        player_payload = {
            "id": poll["id"],
            "question": poll["question"],
            "round": poll["round"],
            "deadline_ms": poll["deadline_ms"],
            "status": "open",
            "group_ids": group_ids,
            "options": [poll_option_public(o) for o in pollmod.poll_options(db, poll["id"])],
            "my_option_id": None,
        }
        await publish(room["code"], room["id"], "poll_created", host_payload,
                      player_payload=player_payload)
        return host_payload


@app.put("/api/join/{join_code}/polls/{poll_id}/vote")
async def cast_poll_vote(join_code: str, poll_id: int, body: PollVoteIn):
    """参与组提交/改投：每组只计一票（UNIQUE(poll_id, group_id) 原地更新）。

    仅投票白名单内的组、在 open 且虚拟时钟未到截止点时可投；截止后
    返回 409（等待 tick 一次性结算）。选项不属于本投票返回 404，
    组不在白名单/投票属于其他房间或旧轮次时按 404 处理，不暴露其存在。
    """
    async with state_lock:
        group = db.execute(
            "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
        ).fetchone()
        if group is None:
            raise HTTPException(status_code=404, detail="join code not found")
        poll = pollmod.get_poll(db, group["room_id"], poll_id)
        if poll is None:
            raise HTTPException(status_code=404, detail="poll not found")
        # 旧轮次投票（已结算/已作废）对参与者不可见
        if poll["round"] != current_round(group["room_id"]):
            raise HTTPException(status_code=404, detail="poll not found")
        if not db.execute(
            "SELECT 1 FROM poll_groups WHERE poll_id = ? AND group_id = ?",
            (poll_id, group["id"]),
        ).fetchone():
            raise HTTPException(status_code=404, detail="poll not found")
        if poll["status"] != "open":
            raise HTTPException(status_code=409, detail=f"poll is {poll['status']}")
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        if poll["deadline_ms"] <= room_elapsed_ms(room):
            # 到期到 tick 结算之间的窄窗口：拒绝写入，结果以结算为准
            raise HTTPException(status_code=409, detail="poll deadline reached")
        option = db.execute(
            "SELECT * FROM poll_options WHERE id = ? AND poll_id = ?",
            (body.option_id, poll_id),
        ).fetchone()
        if option is None:
            raise HTTPException(status_code=404, detail="option not found")
        result = pollmod.cast_vote(db, poll, group["id"], option["id"])
        host_poll = db.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        # 主持人收到含逐组选票的完整投票对象；本组参与者只收脱敏三字段
        # （event_visible 按 group_id 过滤，他组收不到；player_payload 保证
        # 连本组连接也拿不到含他组选票的 poll 全量对象）
        await publish(room["code"], room["id"], "poll_vote_updated",
                      {"poll_id": poll_id, "group_id": group["id"],
                       "option_id": option["id"], "poll": poll_host_view(host_poll)},
                      player_payload={"poll_id": poll_id, "group_id": group["id"],
                                      "option_id": option["id"]})
        return {"poll_id": result.poll_id, "group_id": result.group_id,
                "option_id": result.option_id, "changed": not result.first_vote}


# ---------------------------------------------------------------- REST：应急车辆申请

class VehiclePoolIn(BaseModel):
    total: int = Field(ge=1, le=20)  # 车辆池总量，创建后固定


class VehicleRequestIn(BaseModel):
    quantity: int = Field(ge=1, le=5)  # 申请数量；不得超过池总量（域内再校验）


def _vehicle_player_payloads(room_id: int, extra_group_ids: tuple[int, ...] = ()) -> dict:
    """车辆事件按组分发的脱敏载荷：每组只收本组申请状态（无申请则为 None）。

    覆盖当前仍有申请的全部组（补位/排位可能变化）加上额外受影响组
    （如刚取消申请、刚被删除的组），保证各连接的本组视图及时收敛。
    """
    gids = {row["group_id"] for row in db.execute(
        "SELECT group_id FROM vehicle_requests WHERE room_id = ?", (room_id,)
    )}
    gids.update(extra_group_ids)
    return {gid: {"my_request": vehiclemod.my_view(db, room_id, gid)} for gid in gids}


@app.post("/api/rooms/{code}/vehicle-pool", status_code=201)
async def create_vehicle_pool(code: str, body: VehiclePoolIn):
    """主持人创建车辆池：总量 1～20 辆、创建后固定；每房间仅一个，重置不清池。"""
    async with state_lock:
        room = get_room_or_404(code)
        try:
            vehiclemod.create_pool(db, room["id"], body.total)
        except vehiclemod.VehicleError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        payload = vehiclemod.host_view(db, room["id"])
        # 车辆池总量属主持人编排信息：仅主持人可见（参与者提交时由服务端校验）
        await publish(room["code"], room["id"], "vehicle_pool_created", payload)
        return payload


@app.post("/api/join/{join_code}/vehicle-requests", status_code=201)
async def submit_vehicle_request(join_code: str, body: VehicleRequestIn):
    """参与组提交一份车辆申请（1～5 辆且不超过池总量），取消后才能重新申请。

    服务端在 state_lock 内按受理顺序处理：无更早待分配申请且余量足够才立即
    分配，否则入队等待补位。返回本组申请状态（含排位）。
    """
    async with state_lock:
        group = db.execute(
            "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
        ).fetchone()
        if group is None:
            raise HTTPException(status_code=404, detail="join code not found")
        try:
            row = vehiclemod.submit_request(db, group["room_id"], group["id"], body.quantity)
        except vehiclemod.VehicleError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        # 主持人收全量（各组申请、队列与剩余车辆）；仅提交组收本组脱敏载荷，
        # 他组与观察者不感知本次申请
        await publish(
            room["code"], room["id"], "vehicle_request_submitted",
            {"group_id": group["id"], "quantity": row["quantity"], "status": row["status"],
             "vehicles": vehiclemod.host_view(db, room["id"])},
            player_payloads={group["id"]: {"my_request": vehiclemod.my_view(
                db, room["id"], group["id"])}},
        )
        return vehiclemod.my_view(db, room["id"], group["id"])


@app.delete("/api/join/{join_code}/vehicle-requests")
async def cancel_vehicle_request(join_code: str):
    """参与组取消本组申请（之后可重新申请）。

    取消已分配申请释放车辆后，在同一把锁内从队首依次补位（队首数量不足
    不跳过）；补位与排位变化按组分发脱敏载荷。
    """
    async with state_lock:
        group = db.execute(
            "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
        ).fetchone()
        if group is None:
            raise HTTPException(status_code=404, detail="join code not found")
        try:
            cancelled, newly = vehiclemod.cancel_request(db, group["room_id"], group["id"])
        except vehiclemod.VehicleError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        player_payloads = _vehicle_player_payloads(room["id"], (group["id"],))
        await publish(
            room["code"], room["id"], "vehicle_request_cancelled",
            {"group_id": group["id"], "quantity": cancelled["quantity"],
             "was_status": cancelled["status"],
             "affected_group_ids": sorted(player_payloads),
             "allocated_group_ids": [r["group_id"] for r in newly],
             "vehicles": vehiclemod.host_view(db, room["id"])},
            player_payloads=player_payloads,
        )
        return {"ok": True, "cancelled": cancelled["quantity"]}


# ---------------------------------------------------------------- REST：现场情报核验板

class IntelClaimIn(BaseModel):
    statement: str = Field(min_length=1, max_length=200)                # 待核验命题
    group_ids: list[int] = Field(min_length=intelmod.MIN_CLAIM_GROUPS,
                                 max_length=MAX_GROUPS)                 # 至少两个参与组


class IntelReportIn(BaseModel):
    stance: str = Field(pattern="^(support|oppose)$")                   # 支持 | 反对
    rationale: str = Field(min_length=1, max_length=intelmod.MAX_RATIONALE_LEN)  # 简短依据


async def publish_intel_claim(room: sqlite3.Row, claim_id: int, type_: str) -> None:
    """命题报告/白名单变化后，把即时重算的最新状态分发给各视野。须在 state_lock 下调用。

    主持人收全量（逐组报告与依据正文）；白名单内每个参与组收各自的脱敏载荷
    （本组报告 + 汇总状态，不含他组立场明细）；非白名单组与观察者收不到。
    """
    claim = db.execute("SELECT * FROM intel_claims WHERE id = ?", (claim_id,)).fetchone()
    group_ids = intelmod.claim_group_ids(db, claim_id)
    await publish(
        room["code"], room["id"], type_,
        {"claim_id": claim_id, "group_ids": group_ids, "claim": intel_host_view(claim)},
        player_payloads={
            gid: {"claim_id": claim_id, "claim": intel_player_view(claim, gid)}
            for gid in group_ids
        },
    )


@app.post("/api/rooms/{code}/claims", status_code=201)
async def create_intel_claim(code: str, body: IntelClaimIn):
    """主持人创建待核验命题：指定至少两个参与组（白名单）。

    任意房间状态可创建；命题归属当前轮次（仅作标记），跨重置保留。
    """
    async with state_lock:
        room = get_room_or_404(code)
        statement = body.statement.strip()
        if not statement:
            raise HTTPException(status_code=400, detail="命题不能为空")
        group_ids = list(dict.fromkeys(body.group_ids))  # 去重保序
        if len(group_ids) < intelmod.MIN_CLAIM_GROUPS:
            raise HTTPException(status_code=400, detail="每条命题至少指定两个参与组")
        for gid in group_ids:
            get_group_or_404(room["id"], gid)  # 每个指定组都必须属于本房间
        round_idx = current_round(room["id"])
        claim = intelmod.create_claim(db, room["id"], statement, group_ids, round_idx)
        host_payload = intel_host_view(claim)
        # 参与者通用脱敏载荷：新命题必然零报告（各组 my_report 为空、状态待核验），
        # 广播按白名单过滤
        await publish(room["code"], room["id"], "intel_claim_created", host_payload,
                      player_payload=intel_player_view(claim, None))
        return host_payload


def resolve_group_claim(join_code: str, claim_id: int) -> tuple[sqlite3.Row, sqlite3.Row]:
    """把参与者请求（提交/撤回报告）解析为 (参与组, 命题)。

    参与者凭组加入码操作，且只能操作白名单含本组的命题；
    非白名单命题一律 404，不向参与者暴露其存在。
    """
    group = db.execute(
        "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
    ).fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="join code not found")
    claim = intelmod.get_claim(db, group["room_id"], claim_id)
    if claim is None or not intelmod.group_in_whitelist(db, claim_id, group["id"]):
        raise HTTPException(status_code=404, detail="claim not found")
    return group, claim


@app.put("/api/join/{join_code}/claims/{claim_id}/report")
async def put_intel_report(join_code: str, claim_id: int, body: IntelReportIn):
    """参与组提交/修改本组报告：支持或反对 + 简短依据；同组修改覆盖原报告。

    每组每命题仅一份最新报告（UNIQUE(claim_id, group_id) 原地更新、revision +1），
    同组多次提交绝不计为多份证据。落库后即时重算状态并广播给主持人与各白名单组。
    """
    async with state_lock:
        group, claim = resolve_group_claim(join_code, claim_id)
        try:
            intelmod.submit_report(db, claim, group["id"], body.stance, body.rationale)
        except intelmod.IntelError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        await publish_intel_claim(room, claim_id, "intel_claim_updated")
        return intel_player_view(
            db.execute("SELECT * FROM intel_claims WHERE id = ?", (claim_id,)).fetchone(),
            group["id"],
        )


@app.delete("/api/join/{join_code}/claims/{claim_id}/report")
async def withdraw_intel_report(join_code: str, claim_id: int):
    """参与组撤回本组报告。撤回后即时重算状态并广播；本组没有报告时返回 404。"""
    async with state_lock:
        group, claim = resolve_group_claim(join_code, claim_id)
        withdrawn = intelmod.withdraw_report(db, claim_id, group["id"])
        if withdrawn is None:
            raise HTTPException(status_code=404, detail="本组尚未提交报告")
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        await publish_intel_claim(room, claim_id, "intel_claim_updated")
        return intel_player_view(
            db.execute("SELECT * FROM intel_claims WHERE id = ?", (claim_id,)).fetchone(),
            group["id"],
        )


# ---------------------------------------------------------------- REST：复盘包导出

@app.get("/api/rooms/{code}/review.zip")
async def export_review_zip(code: str):
    """主持人导出复盘 ZIP：按当前事件水位生成自包含离线回放包。

    水位一致性：在 state_lock 内（与所有状态写操作同一把锁）先读
    high_water = MAX(events.id)，随后采集的所有数据均以 id <= high_water
    为准；临界区内不会插入新事件，锁释放后即使立刻有新事件，其 id 也必然
    大于水位——生成期间到达的新事件绝不混入。空演练（无组、无卡、未开始）
    同样能导出并打开。

    包内不含房间码、组加入码与观察 token（share_* 事件整类剔除，
    group_added 载荷去除 join_code）。
    """
    async with state_lock:
        room = get_room_or_404(code)
        high_water = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
        ).fetchone()["m"]
        data = reviewmod.build_review_data(db, room, high_water)
        blob = reviewmod.build_review_zip(data)
    filename = reviewmod.review_filename(room["name"])
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                "attachment; filename*=UTF-8''" + urllib.parse.quote(filename)
            ),
            "Cache-Control": "no-store",
        },
    )


# ---------------------------------------------------------------- REST：响应时效面板

@app.get("/api/rooms/{code}/latency")
async def latency_report(code: str, round_idx: int | None = Query(default=None, alias="round")):
    """主持人专用的响应时效面板：按演练轮次统计各目标组到首次回执/首次答复的虚拟毫秒差。

    与导出同一水位口径：在 state_lock 内（与所有状态写操作同一把锁）先固定
    事件水位 high_water = MAX(events.id)，随后只消费 id <= 水位的事件，
    复用时钟事件重建轮次与虚拟时间（暂停时长不计入）——生成期间到达的新事件
    绝不混入。撤销回执/修改答复不改首次响应时间；未响应为 null（未达），
    不用墙钟补足时延；中位数只取已响应样本，空样本为 null（无数据）。

    仅主持人可获取：房间码即主持人凭证，参与者的组加入码与观察凭证都不是
    房间码，在此路径一律 404；分析数据也不进入任何快照或广播事件。
    """
    async with state_lock:
        room = get_room_or_404(code)
        high_water = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
        ).fetchone()["m"]
        selected = current_round(room["id"]) if round_idx is None else round_idx
        try:
            return latencymod.build_latency_report(
                db, room["id"], selected, high_water, room_elapsed_ms(room))
        except latencymod.LatencyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)


# ---------------------------------------------------------------- REST：时钟控制

async def _clock_action(code: str, expect: tuple[str, ...], action: str, sql: str, params: tuple):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] not in expect:
            raise HTTPException(status_code=409, detail=f"cannot {action} while status={room['status']}")
        db.execute(sql, (*params, room["id"]))
        db.commit()
        fresh = db.execute("SELECT * FROM rooms WHERE id = ?", (room["id"],)).fetchone()
        payload = {"action": action, "status": fresh["status"], "elapsed_ms": room_elapsed_ms(fresh)}
        await publish(room["code"], room["id"], "clock", payload)
        return payload


@app.post("/api/rooms/{code}/start")
async def start_room(code: str):
    return await _clock_action(
        code, ("idle",), "started",
        "UPDATE rooms SET status = 'running', elapsed_ms = 0, last_resume_at = ? WHERE id = ?",
        (time.time(),),
    )


@app.post("/api/rooms/{code}/pause")
async def pause_room(code: str):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] != "running":
            raise HTTPException(status_code=409, detail=f"cannot pause while status={room['status']}")
        elapsed = room_elapsed_ms(room)  # 冻结当前值，暂停期间不再推进
        db.execute(
            "UPDATE rooms SET status = 'paused', elapsed_ms = ?, last_resume_at = NULL WHERE id = ?",
            (elapsed, room["id"]),
        )
        db.commit()
        payload = {"action": "paused", "status": "paused", "elapsed_ms": elapsed}
        await publish(room["code"], room["id"], "clock", payload)
        return payload


@app.post("/api/rooms/{code}/resume")
async def resume_room(code: str):
    return await _clock_action(
        code, ("paused",), "resumed",
        "UPDATE rooms SET status = 'running', last_resume_at = ? WHERE id = ?",
        (time.time(),),
    )


@app.post("/api/rooms/{code}/reset")
async def reset_room(code: str):
    """归零时钟并收回已揭示卡片；参与组、加入码、卡片目标与门槛编排全部保留。

    回执随揭示一并收回：未揭示卡片不可回执，新一轮演练从零开始统计；
    门槛超时标记同时清零，门槛回到等待状态；
    处置答复一并清空并解除全部锁定，新一轮演练从零开始作答。
    现场决策投票：未结算（open）投票一律作废（选票删除、状态置 voided），
    已结算结果按轮次保留（行内 round 决定归属），作废事件仅主持人可见。
    应急车辆申请：全部申请清空（余量恢复全部），车辆池保留供新一轮使用。
    现场情报核验板：全部报告清空（各命题回到待核验），命题与白名单保留。
    """
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="pause before reset")
        db.execute(
            "UPDATE rooms SET status = 'idle', elapsed_ms = 0, last_resume_at = NULL WHERE id = ?",
            (room["id"],),
        )
        db.execute(
            "UPDATE cards SET revealed = 0, revealed_at_ms = NULL, timed_out = 0,"
            " answers_locked = 0 WHERE room_id = ?",
            (room["id"],),
        )
        db.execute("DELETE FROM receipts WHERE room_id = ?", (room["id"],))
        db.execute("DELETE FROM answers WHERE room_id = ?", (room["id"],))
        voided_ids = pollmod.void_open_polls(db, room["id"])
        # 应急车辆：清空全部申请（已分配/排队一并删除，余量恢复全部），车辆池保留
        vehiclemod.reset_requests(db, room["id"])
        # 情报核验：清空全部报告（命题与白名单保留，各命题回到待核验）
        intelmod.reset_reports(db, room["id"])
        # 注意：anchors 表不在清理之列——复盘锚点跨重置保留，
        # 并以行内 round 归属原轮次（当前轮次已由本 reset 事件 +1）。
        db.commit()
        payload = {"action": "reset", "status": "idle", "elapsed_ms": 0}
        await publish(room["code"], room["id"], "reset", payload)
        # 作废事件在 reset 事件之后逐条广播（仅主持人可见）：参与者在收到
        # reset 时已清空本轮投票，这些投票 round 小于新一轮、快照不再下发。
        for poll_id in voided_ids:
            poll = db.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
            await publish(room["code"], room["id"], "poll_voided",
                          {"id": poll_id, "round": poll["round"], "status": "voided"})
        await publish_room_gate_refresh(room)  # 回执已清空，门槛回到等待
        return payload


# ---------------------------------------------------------------- WebSocket

@app.websocket("/ws/{code}")
async def room_ws(websocket: WebSocket, code: str, last_event_id: int = 0):
    """重连协议：客户端带上已应用的最大事件 ID。

    码可以是房间码（主持人，全量视野）或组加入码（参与者，按组过滤）。
    服务端在锁内先登记连接、再发权威快照：快照水位一律取锁内读到的
    「本房间」MAX(events.id)，绝不被客户端提示值抬高——该值可能来自旧库
    或其他房间（事件 id 是全库自增，跨房间不可比），一旦高于本房间水位，
    浏览器会持续丢弃此后的新事件。快照已完整包含水位及之前的全部状态，
    之后的增量事件 id 必然更大，普通重连仍不重复、不回退。
    """
    code = code.upper()
    await websocket.accept()
    async with state_lock:
        room, scope = resolve_code(code)
        if room is None:
            await websocket.send_json({"kind": "error", "message": "room or join code not found"})
            await websocket.close(code=4404)
            return
        hub_key = room["code"]
        hub.add(hub_key, websocket, scope)
        # 快照水位以锁内读取的当前房间水位为准，不采纳客户端 last_event_id
        await websocket.send_json(build_snapshot(room, scope))
    try:
        while True:
            await websocket.receive_text()  # 客户端无需上行；仅用于感知断线
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(hub_key, websocket)


@app.websocket("/ws/observe/{token}")
async def observe_ws(websocket: WebSocket, token: str, last_event_id: int = 0):
    """观察者实时通道：与参与者同一套「先快照后增量」协议，事件按观察视野过滤。

    快照水位同样只认锁内读到的当前房间事件水位，不受客户端 last_event_id
    （可能来自旧库或别的房间）抬高；快照之后的新事件按实际递增 id 正常下发。
    凭证失效（撤销/到期）时拒绝接入；在线连接由撤销/到期路径主动断开（4403）。
    """
    await websocket.accept()
    async with state_lock:
        share = db.execute(
            "SELECT * FROM observer_shares WHERE token = ?", (token,)
        ).fetchone()
        if share is None or not share_active(share):
            await websocket.send_json({"kind": "error", "message": "观察链接已失效或已被撤销"})
            await websocket.close(code=4403)
            return
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (share["room_id"],)).fetchone()
        scope = observer_scope_from(share)
        hub_key = room["code"]
        hub.add(hub_key, websocket, scope)
        # 快照水位以锁内读取的当前房间水位为准，不采纳客户端 last_event_id
        await websocket.send_json(build_snapshot(room, scope))
    try:
        while True:
            await websocket.receive_text()  # 观察者只读，无上行；仅用于感知断线
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(hub_key, websocket)


# ---------------------------------------------------------------- 静态资源

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
