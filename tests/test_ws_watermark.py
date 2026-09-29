"""WebSocket 快照水位协商验证（python3 tests/test_ws_watermark.py）。

与 tests/test_poll_http.py 同一套手法：本机最小环境没有 fastapi/pydantic
（二者在 Docker 镜像内安装），这里用仅含被测路径所需表面的极简 stub 顶替，
然后导入**真实的 app.main**，用假 WebSocket 直接 await 真实路由
room_ws / observe_ws，走真实 state_lock、SQLite、Hub 广播与视野过滤。

修复口径（房间连接 /ws/{code} 与观察连接 /ws/observe/{token} 同协议）：

1. 快照 last_event_id 一律是 state_lock 内读取的「当前房间」MAX(events.id)，
   即使客户端把 last_event_id 抬成来自旧库/其他房间的巨大值（事件 id 全库
   自增、跨房间不可比），快照水位也不被抬高；
2. 快照之后的新事件按实际递增 id 正常应用——抬水连接留在 Hub 中仍会收到
   id 大于真实水位的广播（浏览器 applyEvent 的 `id <= 水位` 判定因此放行）；
3. 普通重连（客户端水位 <= 房间水位）不重复、不回退：快照水位即真实水位，
   快照已包含水位及之前全部状态，后续事件 id 必然更大；
4. 消息结构、REST 快照与角色过滤约定不变；房间/加入码无效仍以 4404 拒绝，
   观察凭证无效仍以 4403 拒绝。
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
import test_poll_http as _stub  # noqa: E402,F401  (import 即安装 stub)

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
    """记录下行消息的假 WebSocket。

    receive_text 永久挂起以保持在线（Hub 中才能收到后续广播）；测试结束时
    由外层取消任务并吸收 CancelledError。
    """

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
        await self._stay.wait()  # 保持在线，直到任务被取消
        return ""


async def _snapshot(handler, ws, *args):
    """运行处理器直到快照发出（或拒绝关闭），随后取消挂起的接收循环。"""
    task = asyncio.ensure_future(handler(ws, *args))
    try:
        # 快照发送在 state_lock 临界区内完成，几个轮次足够
        for _ in range(100):
            await asyncio.sleep(0)
            if ws.sent or ws.closed is not None:
                break
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, main.WebSocketDisconnect):
            pass


def snapshot_of(handler, ws, *args):
    run(_snapshot(handler, ws, *args))
    assert ws.sent and ws.sent[0]["kind"] == "snapshot", ws.sent
    return ws.sent[0]


# ---------------------------------------------------------------- 场景 1

def test_room_ws_inflated_client_hint_does_not_raise_watermark():
    """房间 WS：客户端 last_event_id 来自旧库/其他房间且高于本房间最大 id，
    快照水位仍以锁内当前房间水位为准，且快照后真实新事件照常投递。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))

        # 当前房间先建好：建组事件是本房间第一条事件
        room = run(main.create_room(main.RoomIn(name="本房间")))
        code = room["code"]
        g = run(main.add_group(code, main.GroupIn(name="红队")))
        room_max = main.db.execute(
            "SELECT MAX(id) m FROM events WHERE room_id="
            " (SELECT id FROM rooms WHERE code=?)", (code,)).fetchone()["m"]

        # 随后「其他房间/旧库」产生一批事件，把全库自增序列推到本房间水位之上
        # （events.id 全库自增、跨房间不可比）：客户端若带着他房间水位连入，
        # 提示值就会高于当前房间最大事件 id。
        other = run(main.create_room(main.RoomIn(name="旧房间")))
        rid_other = main.db.execute(
            "SELECT id FROM rooms WHERE code=?", (other["code"],)).fetchone()["id"]
        for _ in range(10):
            run(main.publish(other["code"], rid_other, "anchor_created", {"title": "x"}))
        other_max = main.db.execute("SELECT MAX(id) m FROM events").fetchone()["m"]
        assert other_max > room_max

        inflated = other_max + 5000

        # 携带虚高水位连接：快照水位必须仍是当前房间真实水位
        snap = snapshot_of(main.room_ws, FakeWebSocket(), code, inflated)
        assert snap["last_event_id"] == room_max, snap["last_event_id"]
        assert snap["last_event_id"] < inflated
        # 快照内容仍是本房间全量视野（结构与角色约定不变）
        assert snap["scope"] == {"role": "host", "group_id": None}
        assert snap["room"]["code"] == code
        assert [x["name"] for x in snap["groups"]] == ["红队"]

        # 参与者（组加入码）同样不被抬高
        join_code = g["join_code"]
        psnap = snapshot_of(main.room_ws, FakeWebSocket(), join_code, inflated)
        assert psnap["last_event_id"] == room_max
        assert psnap["scope"]["role"] == "player"
        assert psnap["scope"]["group_id"] == g["id"]
        assert "code" not in psnap["room"]  # 参与者拿不到房间码

        # 快照之后的新事件按实际递增 id 正常应用：虚高水位的在线连接
        # 仍留在 Hub 中，收到 id > 真实水位的重命名事件
        ws = FakeWebSocket()
        task = None

        async def scenario():
            nonlocal task
            task = asyncio.ensure_future(main.room_ws(ws, code, inflated))
            try:
                for _ in range(100):
                    await asyncio.sleep(0)
                    if ws.sent:
                        break
                # 主持人改名组 → 落库 group_renamed（真实递增 id）并广播
                await main.rename_group(code, g["id"], main.GroupIn(name="红队改名"))
                for _ in range(100):
                    await asyncio.sleep(0)
                    if len(ws.sent) >= 2:
                        break
            finally:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, main.WebSocketDisconnect):
                    pass

        run(scenario())

        kinds = [m["kind"] for m in ws.sent]
        assert kinds[0] == "snapshot"
        assert kinds.count("event") >= 1, kinds
        ev = ws.sent[1]
        assert ev["type"] == "group_renamed"
        # 快照之后的事件按实际递增 id 下发：id 是全库序列的真实下一个值，
        # 严格大于快照携带的房间真实水位——浏览器 applyEvent 放行，新事件
        # 不再因客户端虚高提示而被持续丢弃。
        assert ev["id"] == other_max + 1
        assert ev["id"] > ws.sent[0]["last_event_id"] == room_max
        assert ev["id"] < inflated
        h.close()
    print("ok: room ws snapshot watermark ignores inflated client hint; later events flow")


# ---------------------------------------------------------------- 场景 2

def test_observe_ws_inflated_hint_and_invalid_token():
    """观察 WS：虚高 last_event_id 同样不抬高快照水位，新事件正常下发；
    凭证无效维持原有 4403 拒绝。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))
        # 当前房间先建好（建组 + 分享两条事件）
        room = run(main.create_room(main.RoomIn(name="观察房")))
        code = room["code"]
        g = run(main.add_group(code, main.GroupIn(name="红队")))
        share = run(main.create_share(code, main.ShareIn(group_ids=[g["id"]], minutes=30)))
        token = share["token"]
        room_max = main.db.execute(
            "SELECT MAX(id) m FROM events WHERE room_id="
            " (SELECT id FROM rooms WHERE code=?)", (code,)).fetchone()["m"]

        # 其他房间/旧库随后把全库序列推高到当前房间水位之上
        other = run(main.create_room(main.RoomIn(name="旧房间")))
        rid_other = main.db.execute(
            "SELECT id FROM rooms WHERE code=?", (other["code"],)).fetchone()["id"]
        for _ in range(10):
            run(main.publish(other["code"], rid_other, "anchor_created", {"title": "x"}))
        other_max = main.db.execute("SELECT MAX(id) m FROM events").fetchone()["m"]
        assert other_max > room_max

        inflated = other_max + 9999
        snap = snapshot_of(main.observe_ws, FakeWebSocket(), token, inflated)
        assert snap["last_event_id"] == room_max
        assert snap["scope"]["role"] == "observer"
        assert snap["scope"]["group_ids"] == [g["id"]]
        assert "code" not in snap["room"]

        # 快照后真实新事件正常到达观察者（改组名对观察者可见）
        ws = FakeWebSocket()
        task = None

        async def scenario():
            nonlocal task
            task = asyncio.ensure_future(main.observe_ws(ws, token, inflated))
            try:
                for _ in range(100):
                    await asyncio.sleep(0)
                    if ws.sent:
                        break
                await main.rename_group(code, g["id"], main.GroupIn(name="红队二"))
                for _ in range(100):
                    await asyncio.sleep(0)
                    if len(ws.sent) >= 2:
                        break
            finally:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, main.WebSocketDisconnect):
                    pass

        run(scenario())
        assert ws.sent[1]["kind"] == "event"
        assert ws.sent[1]["id"] == other_max + 1
        assert ws.sent[1]["id"] > ws.sent[0]["last_event_id"] == room_max

        # 凭证无效：维持原有拒绝行为（error 帧 + 4403 关闭）
        bad = FakeWebSocket()
        run(_snapshot(main.observe_ws, bad, "no-such-token", inflated))
        assert bad.closed == 4403
        assert bad.sent[0]["kind"] == "error"

        # 撤销后同样 4403
        run(main.revoke_share(code, token))
        revoked = FakeWebSocket()
        run(_snapshot(main.observe_ws, revoked, token, 0))
        assert revoked.closed == 4403
        assert revoked.sent[0]["kind"] == "error"
        h.close()
    print("ok: observe ws ignores inflated hint; invalid/revoked token rejected with 4403")


# ---------------------------------------------------------------- 场景 3

def test_invalid_room_code_rejected_4404():
    """房间/加入码无效：维持原有 error 帧 + 4404 拒绝。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))
        ws = FakeWebSocket()
        run(_snapshot(main.room_ws, ws, "NOPE99", 0))
        assert ws.closed == 4404
        assert ws.sent[0]["kind"] == "error"
        h.close()
    print("ok: unknown room/join code rejected with 4404")


# ---------------------------------------------------------------- 场景 4

def test_normal_reconnect_watermark_is_actual_room_high_water():
    """普通重连：客户端水位 <= 房间水位时，快照水位即锁内真实水位，
    既不抬高也不回退；之后的事件 id 严格更大，浏览器丢弃规则下不重复。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))
        room = run(main.create_room(main.RoomIn(name="普通房")))
        code = room["code"]
        g = run(main.add_group(code, main.GroupIn(name="红队")))
        room_max = main.db.execute(
            "SELECT MAX(id) m FROM events WHERE room_id="
            " (SELECT id FROM rooms WHERE code=?)", (code,)).fetchone()["m"]

        # 客户端水位落后（模拟断线期间错过事件）：快照补齐到真实水位
        behind = snapshot_of(main.room_ws, FakeWebSocket(), code, 0)
        assert behind["last_event_id"] == room_max
        # 客户端水位恰好等于当前水位（普通快速重连）：不抬高、不回退
        caught = snapshot_of(main.room_ws, FakeWebSocket(), code, room_max)
        assert caught["last_event_id"] == room_max
        # 客户端水位略低于当前：仍以真实水位为准
        mid = snapshot_of(main.room_ws, FakeWebSocket(), code, max(0, room_max - 1))
        assert mid["last_event_id"] == room_max
        h.close()
    print("ok: normal reconnect snapshot watermark = actual room high water, no raise/no rollback")


if __name__ == "__main__":
    test_room_ws_inflated_client_hint_does_not_raise_watermark()
    test_observe_ws_inflated_hint_and_invalid_token()
    test_invalid_room_code_rejected_4404()
    test_normal_reconnect_watermark_is_actual_room_high_water()
    print("ALL WS WATERMARK TESTS PASSED")
