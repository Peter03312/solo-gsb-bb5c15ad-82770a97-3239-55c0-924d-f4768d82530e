"""观察席增量视野修复验证（python3 tests/test_observer_view.py）。

与 tests/test_ws_watermark.py 同一套手法：本机最小环境没有 fastapi/pydantic
（二者在 Docker 镜像内安装），这里复用 test_poll_http 的极简 stub 顶替，
然后导入**真实的 app.main**，用假 WebSocket 直接 await 真实路由
observe_ws / room_ws，并直接调用真实 REST 路由函数与 publish，
走真实 state_lock、SQLite、Hub 广播与 event_visible 视野过滤。

本轮修复口径：

1. card_removed 只在「删除前已揭示」且「属于授权组（全员或所选组）」时
   才投递给观察者——删除尚未揭示的全员卡/授权组卡，不得向观察者推送其
   卡片标识；删除非授权组（如未被观察的绿队）卡片观察者同样收不到。
   可见性以事件载荷中的删除前 revealed 判定（删除路径在删行前写入，
   删行后快照只剩已揭示卡，无从判定）。
2. 已揭示卡片删除后，观察页随实时事件同步移除；刷新/重连得到的 REST/WS
   快照与实时结果一致（被删卡不在快照中，未删卡仍在）。
3. answer_lock_updated 对观察者一律不可见（锁定/开放都不推送），
   观察者快照卡片也不再携带 answers_locked 编排字段；
   主持人与参与组的既有事件行为保持不变（参与组仍收本组可见卡片的锁事件）。
4. 主持人始终收到全部 card_removed（含未揭示卡，载荷带删除前 revealed）；
   参与者仍按全员/本组收到 card_removed，既有契约不变。
"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 复用 test_poll_http 的第三方包 stub（pydantic/fastapi 最小表面）
sys.path.insert(0, str(ROOT / "tests"))
import test_poll_http as _stub  # noqa: E402,F402  (import 即安装 stub)

from app import db as dbmod  # noqa: E402
from app import main  # noqa: E402


def run(coro):
    return asyncio.run(coro)


class Harness:
    def __init__(self, db_path):
        os.environ["DATABASE_PATH"] = db_path
        dbmod.DB_PATH = db_path
        main.db = dbmod.connect()
        dbmod.init_schema(main.db)

    def close(self):
        main.db.close()


class FakeWebSocket:
    """记录下行消息的假 WebSocket；receive_text 永久挂起以保持在线。"""

    def __init__(self):
        self.sent = []
        self.closed = None
        self._stay = asyncio.Event()

    async def accept(self):
        return None

    async def send_json(self, message):
        self.sent.append(message)

    async def close(self, code=None):
        self.closed = code

    async def receive_text(self):
        await self._stay.wait()
        return ""


async def spin():
    """让 send_json 等挂起的协程排干。"""
    for _ in range(50):
        await asyncio.sleep(0)


def events(ws, type_=None):
    return [m for m in ws.sent
            if m.get("kind") == "event" and (type_ is None or m.get("type") == type_)]


async def reveal(room_id, card_id, elapsed_ms=10_000):
    """模拟 ticker 到期揭示：更新行并广播 card_revealed（与 ticker_loop 同口径）。"""
    main.db.execute(
        "UPDATE cards SET revealed = 1, revealed_at_ms = ? WHERE id = ?",
        (elapsed_ms, card_id),
    )
    main.db.commit()
    fresh = main.db.execute("SELECT * FROM cards WHERE id = ?", (card_id,)).fetchone()
    room = main.db.execute("SELECT * FROM rooms WHERE id = ?", (room_id,)).fetchone()
    await main.publish(room["code"], room_id, "card_revealed", main.card_public(fresh))


async def scenario():
    """返回 (observer_ws, red_ws, room, groups, cards)。"""
    room = await main.create_room(main.RoomIn(name="观察席视野房"))
    code = room["code"]
    red = await main.add_group(code, main.GroupIn(name="红队"))
    blue = await main.add_group(code, main.GroupIn(name="蓝队"))
    green = await main.add_group(code, main.GroupIn(name="绿队"))

    async def add_card(title, group_id=None, trigger=100):
        return await main.add_card(code, main.CardIn(
            title=title, trigger_seconds=trigger, group_id=group_id))

    c_all_rev = await add_card("全员已揭示", None)          # 观察者应见 → 删除后应实时移除
    c_all_hid = await add_card("全员未揭示", None, 200)     # 未揭示：删除不得通知观察者
    c_red_rev = await add_card("红队已揭示", red["id"])
    c_red_hid = await add_card("红队未揭示", red["id"], 200)
    c_grn_rev = await add_card("绿队已揭示", green["id"])   # 非授权组：观察者始终不见

    # 开始演练并揭示其中三张（与 ticker 到期揭示同一更新 + card_revealed 事件）
    await main.start_room(code)
    room_id = main.db.execute(
        "SELECT id FROM rooms WHERE code = ?", (code,)).fetchone()["id"]
    await reveal(room_id, c_all_rev["id"], 10_000)
    await reveal(room_id, c_red_rev["id"], 10_000)
    await reveal(room_id, c_grn_rev["id"], 10_000)
    # 主持人暂停后再删卡/锁答复（DELETE 卡片接口仅在非 running 时允许）
    await main.pause_room(code)

    # 观察席只授权红队 + 蓝队；另建一个红队参与者连接
    share = await main.create_share(
        code, main.ShareIn(group_ids=[red["id"], blue["id"]], minutes=30))
    token = share["token"]

    obs = FakeWebSocket()
    obs_task = asyncio.ensure_future(main.observe_ws(obs, token, 0))
    red_ws = FakeWebSocket()
    red_task = asyncio.ensure_future(main.room_ws(red_ws, red["join_code"], 0))
    await spin()

    def snapshot_of(ws):
        return next(m for m in ws.sent if m.get("kind") == "snapshot")

    # ---- 初始观察快照：只含 2 张已揭示授权卡，且无 answers_locked 编排字段
    snap = snapshot_of(obs)
    snap_ids = {c["id"] for c in snap["cards"]}
    assert snap_ids == {c_all_rev["id"], c_red_rev["id"]}, snap_ids
    for c in snap["cards"]:
        assert "answers_locked" not in c, c
        assert "receipts" not in c and "answers" not in c
    assert "code" not in snap["room"]

    # ---- 锁定/开放：观察者一律收不到；红队参与者收得到本组可见卡的锁事件
    await main.lock_answers(code, c_red_rev["id"])
    await main.unlock_answers(code, c_red_rev["id"])
    await main.lock_answers(code, c_red_hid["id"])   # 未揭示卡也可预先锁定
    await spin()
    assert events(obs, "answer_lock_updated") == [], \
        [m["payload"] for m in events(obs, "answer_lock_updated")]
    red_lock_ids = {m["payload"]["card_id"]
                    for m in events(red_ws, "answer_lock_updated")}
    assert red_lock_ids == {c_red_rev["id"], c_red_hid["id"]}, red_lock_ids

    # ---- 删除：已揭示全员/红队卡 → 观察者收 card_removed 并据此移除；
    #      未揭示全员/红队卡、非授权组绿队卡 → 观察者收不到删除标识
    await main.delete_card(code, c_all_rev["id"])
    await main.delete_card(code, c_red_rev["id"])
    await main.delete_card(code, c_all_hid["id"])
    await main.delete_card(code, c_red_hid["id"])
    await main.delete_card(code, c_grn_rev["id"])
    await spin()

    removed = {m["payload"]["id"]: m["payload"] for m in events(obs, "card_removed")}
    assert set(removed) == {c_all_rev["id"], c_red_rev["id"]}, set(removed)
    assert all(p["revealed"] is True for p in removed.values()), removed
    # 观察者实时应用 card_removed 后，观察页卡片应同步移除（前端 applyEvent 同口径）
    live_ids = {c["id"] for c in snap["cards"]} - set(removed)
    assert live_ids == set(), live_ids

    # 主持人：全部 5 条 card_removed 都落库且对主持人视野可见，载荷带删除前 revealed
    import json
    for cid, was_revealed in [
        (c_all_rev["id"], True), (c_red_rev["id"], True),
        (c_all_hid["id"], False), (c_red_hid["id"], False),
        (c_grn_rev["id"], True),
    ]:
        row = main.db.execute(
            "SELECT payload FROM events WHERE type='card_removed'"
            " AND json_extract(payload, '$.id') = ?", (cid,)).fetchone()
        assert row is not None, cid
        p = json.loads(row["payload"])
        assert p["revealed"] == was_revealed, p
        assert main.event_visible(None, "card_removed", p) is True  # 主持人全收

    # 红队参与者：全员卡 + 红队卡（含未揭示）的删除都收得到，绿队卡收不到
    red_removed = {m["payload"]["id"] for m in events(red_ws, "card_removed")}
    assert red_removed == {
        c_all_rev["id"], c_red_rev["id"], c_all_hid["id"], c_red_hid["id"]}, red_removed

    for t in (obs_task, red_task):
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, main.WebSocketDisconnect):
            pass

    return token


def test_observer_incremental_visibility():
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))
        token = run(scenario())

        # ---- 刷新（REST 快照）：被删卡全部不在；未删卡仍在；无锁定字段
        async def rest_checks():
            obs_rest = await main.observe_snapshot(token)
            assert obs_rest["scope"]["role"] == "observer"
            assert [c["id"] for c in obs_rest["cards"]] == [], obs_rest["cards"]
            for c in obs_rest["cards"]:
                assert "answers_locked" not in c

        run(rest_checks())
        h.close()
    print("ok: observer card_removed only for pre-deletion revealed authorized cards; "
          "answer_lock_updated never delivered; REST/refresh view matches realtime")


def test_observer_view_consistency_after_partial_delete():
    """删除部分已揭示授权卡：实时移除与刷新快照严格一致，未删卡保留。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))

        async def inner():
            room = await main.create_room(main.RoomIn(name="部分删除房"))
            code = room["code"]
            red = await main.add_group(code, main.GroupIn(name="红队"))
            blue = await main.add_group(code, main.GroupIn(name="蓝队"))
            c1 = await main.add_card(code, main.CardIn(
                title="全员卡A", trigger_seconds=10))
            c2 = await main.add_card(code, main.CardIn(
                title="红队卡B", trigger_seconds=10, group_id=red["id"]))
            await main.start_room(code)
            rid = main.db.execute(
                "SELECT id FROM rooms WHERE code=?", (code,)).fetchone()["id"]
            await reveal(rid, c1["id"], 10_000)
            await reveal(rid, c2["id"], 10_000)
            await main.pause_room(code)
            share = await main.create_share(
                code, main.ShareIn(group_ids=[red["id"], blue["id"]], minutes=30))

            obs = FakeWebSocket()
            task = asyncio.ensure_future(main.observe_ws(obs, share["token"], 0))
            await spin()
            snap = next(m for m in obs.sent if m.get("kind") == "snapshot")
            assert {c["id"] for c in snap["cards"]} == {c1["id"], c2["id"]}

            # 只删红队卡 B（已揭示、授权）：观察者实时只移除 B，A 保留
            await main.delete_card(code, c2["id"])
            await spin()
            removed = {m["payload"]["id"] for m in events(obs, "card_removed")}
            assert removed == {c2["id"]}, removed
            live = {c["id"] for c in snap["cards"]} - removed
            assert live == {c1["id"]}, live

            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, main.WebSocketDisconnect):
                pass

            # 重连（新 WS 快照）与 REST 快照：只剩 A，与实时结果一致
            rest = await main.observe_snapshot(share["token"])
            assert [c["id"] for c in rest["cards"]] == [c1["id"]], rest["cards"]

            ws2 = FakeWebSocket()
            t2 = asyncio.ensure_future(main.observe_ws(ws2, share["token"], 0))
            await spin()
            snap2 = next(m for m in ws2.sent if m.get("kind") == "snapshot")
            assert [c["id"] for c in snap2["cards"]] == [c1["id"]], snap2["cards"]
            t2.cancel()
            try:
                await t2
            except (asyncio.CancelledError, main.WebSocketDisconnect):
                pass

        run(inner())
        h.close()
    print("ok: observer realtime removal equals reconnect/REST snapshot; surviving cards kept")


def test_prelocked_unrevealed_card_does_not_leak_lock_on_reveal():
    """主持人可对未揭示卡预先锁定；揭示时观察者收到的 card_revealed 载荷
    不得带 answers_locked 编排字段，重连快照同样无该字段（实时与刷新一致）。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))

        async def inner():
            room = await main.create_room(main.RoomIn(name="预锁定房"))
            code = room["code"]
            red = await main.add_group(code, main.GroupIn(name="红队"))
            card = await main.add_card(code, main.CardIn(
                title="将被预锁定的全员卡", trigger_seconds=10))
            # 未揭示即锁定（主持人编排动作），再开始并到期揭示
            await main.lock_answers(code, card["id"])
            await main.start_room(code)
            rid = main.db.execute(
                "SELECT id FROM rooms WHERE code=?", (code,)).fetchone()["id"]
            share = await main.create_share(
                code, main.ShareIn(group_ids=[red["id"]], minutes=30))

            obs = FakeWebSocket()
            task = asyncio.ensure_future(main.observe_ws(obs, share["token"], 0))
            await spin()
            await reveal(rid, card["id"], 10_000)
            await spin()

            rev = events(obs, "card_revealed")
            assert len(rev) == 1, [m.get("type") for m in events(obs)]
            assert "answers_locked" not in rev[0]["payload"], rev[0]["payload"]
            # 锁状态再变化观察者也不感知
            await main.pause_room(code)
            await main.unlock_answers(code, card["id"])
            await spin()
            assert events(obs, "answer_lock_updated") == []

            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, main.WebSocketDisconnect):
                pass

            # 重连快照：已揭示卡在列，但同样无 answers_locked
            rest = await main.observe_snapshot(share["token"])
            assert [c["id"] for c in rest["cards"]] == [card["id"]]
            assert "answers_locked" not in rest["cards"][0]

        run(inner())
        h.close()
    print("ok: pre-lock on unrevealed card never reaches observer via card_revealed or snapshot")


if __name__ == "__main__":
    test_observer_incremental_visibility()
    test_observer_view_consistency_after_partial_delete()
    test_prelocked_unrevealed_card_does_not_leak_lock_on_reveal()
    print("ALL OBSERVER VIEW TESTS PASSED")
