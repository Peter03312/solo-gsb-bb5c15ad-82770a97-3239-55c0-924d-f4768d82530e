"""已结算投票结果保存/读取的 HTTP 一致性验证（python3 tests/test_poll_http.py）。

本机最小环境没有 fastapi/pydantic（二者在 Docker 镜像内安装），这里用仅含
被测路径所需表面的极简 stub 顶替这两个第三方包，然后导入**真实的 app.main**，
直接调用其 async 路由函数（FastAPI 运行时对它们的调用方式就是
`await handler(path_args..., body_model)`），并走真实的 state_lock、SQLite、
快照构造 poll_host_view / poll_player_view 与删除组级联路径，验证需求口径：

1. 结算后删除投过票的参与组：主持人 GET /api/rooms/{code} 与仍有资格参与组
   GET /api/join/{code} 读到的获胜项/各选项票数一致，且都是结算当时的固定值，
   不出现「获胜项 0 票、另一项 1 票」；
2. 刷新（重新拉快照）与模拟重启（关闭连接、init_schema 后重连）结果不变；
3. 未结算投票删组仍按旧规则：该组白名单资格与选票级联删除，剩余票实时重算；
4. 旧库缺 poll_results 时从 poll_settled 事件恢复；事件缺失则保留获胜项、
   tallies 为 null 且 tallies_recoverable=false，两端口一致且不拿残余票伪造。

真实部署中这些处理函数由 uvicorn 挂在既有 HTTP 路径上（见 README 调用示例）。
"""
import asyncio
import json
import os
import sys
import tempfile
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ------------------------------------------------------------ 第三方包 stub
def _install_stubs() -> None:
    # ---- pydantic 最小实现：BaseModel(**kwargs) 做类型转换与 Field 约束校验
    import typing

    pydantic = types.ModuleType("pydantic")

    class _FieldInfo:
        def __init__(self, default=..., **constraints):
            self.has_default = default is not ...
            self.default = None if default is ... else default
            self.constraints = constraints

    def Field(default=..., **constraints):  # noqa: N802 - 镜像 pydantic API
        return _FieldInfo(default, **constraints)

    class ValidationError(Exception):
        pass

    class BaseModel:
        def __init__(self, **data):
            ann = typing.get_type_hints(type(self))
            for name, ftype in ann.items():
                fi = getattr(type(self), name, None)
                if name in data:
                    value = data[name]
                elif isinstance(fi, _FieldInfo) and fi.has_default:
                    value = fi.default
                elif isinstance(fi, _FieldInfo):
                    raise ValidationError(f"{name} required")
                else:
                    value = None
                value = self._coerce(name, value, ftype, fi)
                setattr(self, name, value)

        @staticmethod
        def _coerce(name, value, ftype, fi):
            c = getattr(fi, "constraints", {}) if isinstance(fi, _FieldInfo) else {}
            if ftype in (int,) and not isinstance(value, bool):
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    raise ValidationError(f"{name} must be int")
            if ftype is str:
                value = str(value)
            if "min_length" in c and value is not None and len(value) < c["min_length"]:
                raise ValidationError(f"{name} too short")
            if "max_length" in c and value is not None and len(value) > c["max_length"]:
                raise ValidationError(f"{name} too long")
            if "ge" in c and value is not None and value < c["ge"]:
                raise ValidationError(f"{name} < ge")
            if "le" in c and value is not None and value > c["le"]:
                raise ValidationError(f"{name} > le")
            return value

        def dict(self):
            return {k: getattr(self, k) for k in typing.get_type_hints(type(self))}

    pydantic.BaseModel = BaseModel
    pydantic.Field = Field
    pydantic.ValidationError = ValidationError
    sys.modules["pydantic"] = pydantic

    # ---- fastapi 最小实现：仅收集路由（不实际起 HTTP 服务）
    fastapi = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code, detail=None):
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def __init__(self, title="", lifespan=None):
            self.title = title
            self.lifespan = lifespan

        def _register(self, path, *args, **_kw):
            # 同时兼容 @app.post("/x") 与 @app.post("/x", status_code=201)
            if args and callable(args[0]):
                return args[0]
            def decorator(fn):
                return fn
            return decorator

        get = post = put = patch = delete = websocket = _register

        def mount(self, *a, **k):
            return None

    class Query:  # noqa: N801 - 镜像 fastapi.Query(default=..., alias=...)
        def __init__(self, default=None, alias=None, **_kw):
            self.default = default
            self.alias = alias

    class WebSocket:
        pass

    class WebSocketDisconnect(Exception):
        pass

    fastapi.FastAPI = FastAPI
    fastapi.HTTPException = HTTPException
    fastapi.Query = Query
    fastapi.WebSocket = WebSocket
    fastapi.WebSocketDisconnect = WebSocketDisconnect
    sys.modules["fastapi"] = fastapi

    responses = types.ModuleType("fastapi.responses")

    class FileResponse:
        def __init__(self, *a, **k):
            pass

    class StreamingResponse:
        def __init__(self, *a, **k):
            pass

    responses.FileResponse = FileResponse
    responses.StreamingResponse = StreamingResponse
    sys.modules["fastapi.responses"] = responses

    static = types.ModuleType("fastapi.staticfiles")

    class StaticFiles:
        def __init__(self, directory=None):
            self.directory = directory

    static.StaticFiles = StaticFiles
    sys.modules["fastapi.staticfiles"] = static


_install_stubs()

# 将桩就位后再导入真实主程序
from app import db as dbmod  # noqa: E402
from app import main  # noqa: E402
from app import polls as pm  # noqa: E402


def run(coro):
    return asyncio.run(coro)


class Harness:
    def __init__(self, db_path):
        os.environ["DATABASE_PATH"] = db_path
        # main 在 import 时只声明 db；此处复刻 lifespan 启动动作
        dbmod.DB_PATH = db_path
        main.db = dbmod.connect()
        dbmod.init_schema(main.db)

    def close(self):
        main.db.close()


# ---------------------------------------------------------------- 场景 1/2

def test_settled_result_consistent_after_group_delete_refresh_restart():
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))
        # 主持人建房间、两个组、发起投票（截止 10s）
        room = run(main.create_room(main.RoomIn(name="投票房")))
        code = room["code"]
        g1 = run(main.add_group(code, main.GroupIn(name="红队")))
        g2 = run(main.add_group(code, main.GroupIn(name="蓝队")))
        c1 = dbmod_connect_join_code(h, g1["id"])
        c2 = dbmod_connect_join_code(h, g2["id"])
        poll_id = run(main.create_poll(code, main.PollIn(
            question="处置方案", options=["切流", "回滚"],
            group_ids=[g1["id"], g2["id"]], deadline_seconds=10)))["id"]
        opts = [(o["id"], o["seq"]) for o in pm.poll_options(main.db, poll_id)]
        oa = opts[0][0]
        ob = opts[1][0]

        # 两组各投一项：红队→A、蓝队→B
        run(main.cast_poll_vote(c1, poll_id, main.PollVoteIn(option_id=oa)))
        run(main.cast_poll_vote(c2, poll_id, main.PollVoteIn(option_id=ob)))

        # 开始演练并在虚拟时钟到 10s 时由 tick 路径一次性结算（直接调用域函数，
        # 等价于 ticker_loop 锁内行为；结算事件也落库）
        run(main.start_room(code))
        room_row = main.db.execute("SELECT * FROM rooms WHERE code=?", (code,)).fetchone()
        rid = room_row["id"]
        # 让墙钟流逝一段无意义：elapsed 以虚拟毫秒 10_000 传入
        settled = pm.settle_due_polls(main.db, rid, 0, 10_000)
        assert len(settled) == 1
        # 结算事件落库（旧库恢复的数据源）
        async def publish_settled():
            payload = main.poll_host_view(settled[0].poll)
            return await main.publish(code, rid, "poll_settled", payload)
        run(publish_settled())
        # 平票 → 按选项顺序 A 获胜
        assert settled[0].poll["winner_option_id"] == oa

        def host_view():
            snap = run(main.get_room(code))
            return next(p for p in snap["polls"] if p["id"] == poll_id)

        def player_view(join_code):
            snap = run(main.join_room(join_code))
            return next(p for p in snap["polls"] if p["id"] == poll_id)

        hp = host_view()
        assert hp["status"] == "settled"
        assert {t["option_id"]: t["count"] for t in hp["tallies"]} == {oa: 1, ob: 1}
        assert hp["winner_option_id"] == oa
        assert hp["tallies_recoverable"] is True
        # 白名单组读到的汇总与主持人一致（只是没有逐组 votes）
        for jc in (c1, c2):
            pp = player_view(jc)
            assert pp["winner_option_id"] == oa
            assert {t["option_id"]: t["count"] for t in pp["tallies"]} == {oa: 1, ob: 1}
            assert pp["tallies_recoverable"] is True
        assert {pp_my(player_view(c1)), pp_my(player_view(c2))} == {oa, ob}

        # 删除投了获胜项 A 的红队：其选票级联删除，残余只剩 B 一票
        run(main.delete_group(code, g1["id"]))
        residual = main.db.execute(
            "SELECT option_id, COUNT(*) c FROM poll_votes WHERE poll_id=? GROUP BY option_id",
            (poll_id,)).fetchall()
        assert [(r["option_id"], r["c"]) for r in residual] == [(ob, 1)]

        # 删组后「刷新」：主持人与仍有资格的蓝队读到的仍是结算当时固定结果，
        # 绝不出现获胜项 A 零票、B 一票的矛盾
        hp2 = host_view()
        assert hp2["winner_option_id"] == oa
        assert {t["option_id"]: t["count"] for t in hp2["tallies"]} == {oa: 1, ob: 1}
        assert {(v["group_id"], v["option_id"]) for v in hp2["votes"]} == {
            (g1["id"], oa), (g2["id"], ob)}
        pp2 = player_view(c2)
        assert pp2["winner_option_id"] == oa
        assert {t["option_id"]: t["count"] for t in pp2["tallies"]} == {oa: 1, ob: 1}
        assert pp2["my_option_id"] == ob

        # 模拟重启：关连接 → init_schema → 重连读取
        main.db.close()
        main.db = dbmod.connect()
        dbmod.init_schema(main.db)
        hp3 = run(main.get_room(code))
        p3 = next(p for p in hp3["polls"] if p["id"] == poll_id)
        assert p3["winner_option_id"] == oa
        assert {t["option_id"]: t["count"] for t in p3["tallies"]} == {oa: 1, ob: 1}
        pp3 = run(main.join_room(c2))
        q3 = next(p for p in pp3["polls"] if p["id"] == poll_id)
        assert {t["option_id"]: t["count"] for t in q3["tallies"]} == {oa: 1, ob: 1}
        assert q3["winner_option_id"] == oa
        h.close()
    print("ok: HTTP host/player reads fixed settlement after delete/refresh/restart")


def pp_my(player_poll):
    return player_poll["my_option_id"]


def dbmod_connect_join_code(h, group_id):
    row = main.db.execute("SELECT join_code FROM groups WHERE id=?", (group_id,)).fetchone()
    return row["join_code"]


# ---------------------------------------------------------------- 场景 3

def test_open_poll_group_delete_still_cascades_live_tallies():
    """未结算投票：删除参与组仍按旧规则级联其资格与选票，剩余票实时重算，
    截止前改投、最高票获胜、平票按选项顺序的规则不变。"""
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "t.db"))
        room = run(main.create_room(main.RoomIn(name="未结算房")))
        code = room["code"]
        g1 = run(main.add_group(code, main.GroupIn(name="红队")))
        g2 = run(main.add_group(code, main.GroupIn(name="蓝队")))
        c1 = dbmod_connect_join_code(h, g1["id"])
        c2 = dbmod_connect_join_code(h, g2["id"])
        poll_id = run(main.create_poll(code, main.PollIn(
            question="Q", options=["A", "B", "C"],
            group_ids=[g1["id"], g2["id"]], deadline_seconds=100)))["id"]
        opts = [o["id"] for o in sorted(pm.poll_options(main.db, poll_id), key=lambda o: o["seq"])]
        oa, ob, oc = opts
        run(main.cast_poll_vote(c1, poll_id, main.PollVoteIn(option_id=oa)))
        run(main.cast_poll_vote(c2, poll_id, main.PollVoteIn(option_id=ob)))
        # 截止前改投：红队 A → C，票数原地迁移
        run(main.cast_poll_vote(c1, poll_id, main.PollVoteIn(option_id=oc)))
        hp = run(main.get_room(code))
        p = next(x for x in hp["polls"] if x["id"] == poll_id)
        assert {t["option_id"]: t["count"] for t in p["tallies"]} == {oa: 0, ob: 1, oc: 1}

        # 删除红队（投 C）：白名单资格与选票级联，实时汇总只剩 B 一票
        run(main.delete_group(code, g1["id"]))
        hp = run(main.get_room(code))
        p = next(x for x in hp["polls"] if x["id"] == poll_id)
        assert p["group_ids"] == [g2["id"]]
        assert {t["option_id"]: t["count"] for t in p["tallies"]} == {oa: 0, ob: 1, oc: 0}
        assert p["votes"] == [{"group_id": g2["id"], "option_id": ob}]
        # 未结算 → 无固定快照行
        assert pm.fixed_result(main.db, poll_id) is None

        # 截止结算（剩余 B 一票 → B 获胜），结算后才固定
        rid = main.db.execute("SELECT id FROM rooms WHERE code=?", (code,)).fetchone()["id"]
        res = pm.settle_due_polls(main.db, rid, 0, 100_000)
        assert len(res) == 1 and res[0].winner["id"] == ob
        assert res[0].tallies == {oa: 0, ob: 1, oc: 0}
        h.close()
    print("ok: open poll deletes group eligibility/votes; live tallies, settle rules unchanged")


# ---------------------------------------------------------------- 场景 4

def test_legacy_missing_event_both_views_marked_unrecoverable():
    """旧库：已结算行无 poll_results 且无 poll_settled 事件——重启恢复后
    主持人与参与组都只看到保留的获胜项，tallies=null、recoverable=false。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        h = Harness(path)
        room = run(main.create_room(main.RoomIn(name="旧库缺事件房")))
        code = room["code"]
        g1 = run(main.add_group(code, main.GroupIn(name="红队")))
        g2 = run(main.add_group(code, main.GroupIn(name="蓝队")))
        c1 = dbmod_connect_join_code(h, g1["id"])
        rid = main.db.execute("SELECT id FROM rooms WHERE code=?", (code,)).fetchone()["id"]
        # 直接构造旧版已结算行（只有 winner，没有 poll_results / 结算事件）
        poll = pm.create_poll(main.db, rid, "旧", ["A", "B"], [g1["id"], g2["id"]], 10_000, 0)
        oa, ob = pm.poll_options(main.db, poll["id"])
        # 残余票：只剩 B 一票（A 的票随红队删除而级联消失），但旧版 winner 固定为 A
        main.db.execute(
            "INSERT INTO poll_votes (room_id,poll_id,group_id,option_id,round,updated_at)"
            " VALUES (?,?,?,?,0,?)", (rid, poll["id"], g2["id"], ob["id"], time.time()))
        main.db.execute(
            "UPDATE polls SET status='settled', winner_option_id=?, settled_at_ms=10000 WHERE id=?",
            (oa["id"], poll["id"]))
        main.db.execute("DELETE FROM poll_groups WHERE poll_id=? AND group_id=?",
                        (poll["id"], g1["id"]))
        main.db.commit()
        h.close()

        # 重启恢复：无事件 → 标记不可恢复
        main.db = dbmod.connect()
        dbmod.init_schema(main.db)
        hp = run(main.get_room(code))
        p = next(x for x in hp["polls"] if x["id"] == poll["id"])
        assert p["winner_option_id"] == oa["id"]
        assert p["tallies"] is None and p["votes"] is None
        assert p["tallies_recoverable"] is False
        pp = run(main.join_room(_join_code(g2["id"])))
        q = next(x for x in pp["polls"] if x["id"] == poll["id"])
        assert q["winner_option_id"] == oa["id"]
        assert q["tallies"] is None and q["tallies_recoverable"] is False
        main.db.close()

    # 有事件：恢复成功，两端口读到恢复票数
    with tempfile.TemporaryDirectory() as d2:
        path = os.path.join(d2, "t.db")
        h = Harness(path)
        room = run(main.create_room(main.RoomIn(name="旧库有事件房")))
        code = room["code"]
        g1 = run(main.add_group(code, main.GroupIn(name="红队")))
        c1 = dbmod_connect_join_code(h, g1["id"])
        rid = main.db.execute("SELECT id FROM rooms WHERE code=?", (code,)).fetchone()["id"]
        poll = pm.create_poll(main.db, rid, "旧", ["A", "B"], [g1["id"]], 10_000, 0)
        oa, ob = pm.poll_options(main.db, poll["id"])
        main.db.execute(
            "UPDATE polls SET status='settled', winner_option_id=?, settled_at_ms=10000 WHERE id=?",
            (oa["id"], poll["id"]))
        main.db.execute(
            "INSERT INTO events (room_id,type,payload,created_at) VALUES (?,?,?,?)",
            (rid, "poll_settled", json.dumps({
                "id": poll["id"], "status": "settled", "winner_option_id": oa["id"],
                "tallies": [{"option_id": oa["id"], "count": 1},
                            {"option_id": ob["id"], "count": 0}],
                "votes": [{"group_id": g1["id"], "option_id": oa["id"]}]},
                ensure_ascii=False), time.time()))
        main.db.commit()
        h.close()

        main.db = dbmod.connect()
        dbmod.init_schema(main.db)
        hp = run(main.get_room(code))
        p = next(x for x in hp["polls"] if x["id"] == poll["id"])
        assert {t["option_id"]: t["count"] for t in p["tallies"]} == {oa["id"]: 1, ob["id"]: 0}
        assert p["tallies_recoverable"] is True
        pp = run(main.join_room(c1))
        q = next(x for x in pp["polls"] if x["id"] == poll["id"])
        assert {t["option_id"]: t["count"] for t in q["tallies"]} == {oa["id"]: 1, ob["id"]: 0}
        assert q["my_option_id"] == oa["id"]
        main.db.close()
    print("ok: legacy missing event marks unrecoverable both views; event restores tally")


def _join_code(group_id):
    return main.db.execute("SELECT join_code FROM groups WHERE id=?", (group_id,)).fetchone()["join_code"]


if __name__ == "__main__":
    test_settled_result_consistent_after_group_delete_refresh_restart()
    test_open_poll_group_delete_still_cascades_live_tallies()
    test_legacy_missing_event_both_views_marked_unrecoverable()
    print("ALL POLL HTTP TESTS PASSED")
