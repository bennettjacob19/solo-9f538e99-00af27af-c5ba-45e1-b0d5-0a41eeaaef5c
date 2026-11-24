"""/api/catalog 维护后重试流量的模拟限速器。

业务规则（模拟时钟，单位：模拟秒）：

* 开关默认关闭。开启后每个模拟秒最多放行 ``config.CATALOG_RATE_PER_SECOND`` 个
  GET /api/catalog；普通请求只能使用“扣掉已放行和未兑预约之后”的余量。
* 余量用尽 → 429，附带新预约号与 ``Retry-After``；预约落在“从下一秒起最早有余量
  的一秒”，若请求指定了最早重试时刻则跳过更早的时段。
* 凭号重试只能在约定的那一秒兑现，且同一个预约号最多放行一次（并发由
  ``BEGIN IMMEDIATE`` + 条件 UPDATE 双重保证）。
* 退号释放名额；尚未到时的空位可被后续预约重新使用。
* 路由停用时网关先返回 503，不发号也不扣预约；关闭限速即作废本轮全部预约。

效率约束：找空档读取的时段数据量不得随“连续满额的秒数”线性增长，也不得给未预约
的未来秒预建记录。实现上用 ``rl_full_runs``（满额连续区间表）做索引区间探测——
首个空档只被至多一个 run 覆盖，所以是 O(1) 行读取；普通秒只有真正放行时才在
``rl_passed`` 建行，未来秒不会有任何行。
"""
from __future__ import annotations

import time
from typing import Any

from . import config, db

# 预约号前缀，便于和业务请求号区分
RESV_PREFIX = "RSV"


class RateLimitError(Exception):
    """限速器业务拒绝。``status`` 是建议的 HTTP 状态码。"""

    def __init__(self, status: int, code: str, message: str, extra: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra or {}


# ---------------------------------------------------------------------------
# 只读辅助
# ---------------------------------------------------------------------------

def _config_row(con) -> dict[str, Any]:
    row = con.execute("SELECT * FROM rl_config WHERE id=1").fetchone()
    if row is None:  # 旧库升级兜底
        now = db.sim_now(con)
        con.execute(
            "INSERT OR IGNORE INTO rl_config(id, enabled, epoch, updated_sim, updated_real) "
            "VALUES(1, 0, 1, ?, ?)",
            (now, time.time()),
        )
        row = con.execute("SELECT * FROM rl_config WHERE id=1").fetchone()
    return dict(row)


def _reserved_held(con, epoch: int, slot: int) -> int:
    """该秒仍占名额的预约数（reserved 即未兑；已兑 / 已退 / 过期都不再占）。"""
    row = con.execute(
        "SELECT COUNT(*) AS n FROM rl_reservations "
        "WHERE epoch=? AND slot=? AND status='reserved'",
        (epoch, slot),
    ).fetchone()
    return int(row["n"])


def _passed(con, epoch: int, slot: int) -> int:
    row = con.execute(
        "SELECT passed FROM rl_passed WHERE epoch=? AND slot=?", (epoch, slot)
    ).fetchone()
    return int(row["passed"]) if row is not None else 0


def _used(con, epoch: int, slot: int) -> int:
    """该秒已占用名额 = 已放行（含凭号兑现）+ 未兑预约。"""
    return _passed(con, epoch, slot) + _reserved_held(con, epoch, slot)


def _mark_full(con, epoch: int, slot: int) -> None:
    """把一个刚满的秒并入满额连续区间（与相邻 run 合并，区间数不随秒数增长）。

    幂等：该秒已被某个 run 覆盖时直接返回（同一秒可能先因预约预占变满、
    兑现时再次触发标记）。
    """
    existing = con.execute(
        "SELECT 1 FROM rl_full_runs WHERE epoch=? AND start_slot<=? AND end_slot>=? LIMIT 1",
        (epoch, slot, slot),
    ).fetchone()
    if existing is not None:
        return
    left = con.execute(
        "SELECT id, start_slot, end_slot FROM rl_full_runs "
        "WHERE epoch=? AND end_slot=? LIMIT 1", (epoch, slot - 1)
    ).fetchone()
    right = con.execute(
        "SELECT id, start_slot, end_slot FROM rl_full_runs "
        "WHERE epoch=? AND start_slot=? LIMIT 1", (epoch, slot + 1)
    ).fetchone()
    if left is not None and right is not None:
        con.execute(
            "UPDATE rl_full_runs SET end_slot=? WHERE id=?",
            (int(right["end_slot"]), int(left["id"])),
        )
        con.execute("DELETE FROM rl_full_runs WHERE id=?", (int(right["id"]),))
    elif left is not None:
        con.execute(
            "UPDATE rl_full_runs SET end_slot=? WHERE id=?",
            (slot, int(left["id"])),
        )
    elif right is not None:
        con.execute(
            "UPDATE rl_full_runs SET start_slot=? WHERE id=?",
            (slot, int(right["id"])),
        )
    else:
        con.execute(
            "INSERT INTO rl_full_runs(epoch, start_slot, end_slot) VALUES(?, ?, ?)",
            (epoch, slot, slot),
        )


def _release_slot(con, epoch: int, slot: int) -> None:
    """退号释放后把该秒从满额区间移除（区间可能因此裂成两段）。"""
    run = con.execute(
        "SELECT id, start_slot, end_slot FROM rl_full_runs "
        "WHERE epoch=? AND start_slot<=? AND end_slot>=? LIMIT 1",
        (epoch, slot, slot),
    ).fetchone()
    if run is None:
        return
    rid, start, end = int(run["id"]), int(run["start_slot"]), int(run["end_slot"])
    if start == end:
        con.execute("DELETE FROM rl_full_runs WHERE id=?", (rid,))
    elif slot == start:
        con.execute("UPDATE rl_full_runs SET start_slot=? WHERE id=?", (start + 1, rid))
    elif slot == end:
        con.execute("UPDATE rl_full_runs SET end_slot=? WHERE id=?", (end - 1, rid))
    else:
        # 中间秒被释放：原区间缩短为左半，另建右半
        con.execute("UPDATE rl_full_runs SET end_slot=? WHERE id=?", (slot - 1, rid))
        con.execute(
            "INSERT INTO rl_full_runs(epoch, start_slot, end_slot) VALUES(?, ?, ?)",
            (epoch, slot + 1, end),
        )


def _earliest_free_slot(con, epoch: int, now: int, not_before: int) -> int:
    """从 ``max(now+1, not_before)`` 起找最早有余量的一秒。

    利用满额区间索引：候选秒若被某个满额 run 覆盖，就直接跳到该 run 之后再探一次。
    首个空档之前至多跨越 1 个 run，因此读取行数为 O(1)，与连续满额秒数无关；
    未预约 / 未满的未来秒在任何表里都没有行，不会被预建。
    """
    candidate = max(now + 1, not_before)
    while True:
        run = con.execute(
            "SELECT start_slot, end_slot FROM rl_full_runs "
            "WHERE epoch=? AND start_slot<=? AND end_slot>=? LIMIT 1",
            (epoch, candidate, candidate),
        ).fetchone()
        if run is None:
            return candidate
        candidate = int(run["end_slot"]) + 1


def _expire_stale(con, epoch: int, now: int) -> int:
    """把约定秒已过、仍未兑现的预约标记 expired（惰性清理，不影响历史计数）。

    注意是严格小于：约定秒 == 当前模拟秒时预约仍然有效，必须保留到时钟越过该秒。
    """
    cur = con.execute(
        "UPDATE rl_reservations SET status='expired', updated_real=? "
        "WHERE epoch=? AND status='reserved' AND slot<?",
        (time.time(), epoch, now),
    )
    return cur.rowcount


# ---------------------------------------------------------------------------
# 对外操作（每个方法一个 IMMEDIATE 短事务，写锁串行化并发）
# ---------------------------------------------------------------------------

def snapshot() -> dict[str, Any]:
    """供 /api/state 与页面展示的只读快照。"""
    with db.session() as con:
        cfg = _config_row(con)
        epoch, now = int(cfg["epoch"]), db.sim_now(con)
        enabled = bool(cfg["enabled"])
        limit = config.CATALOG_RATE_PER_SECOND
        out: dict[str, Any] = {
            "enabled": enabled,
            "epoch": epoch,
            "rate_per_second": limit,
            "route": config.CATALOG_RATE_ROUTE,
            "sim_now": now,
            "current_slot": None,
            "reservations": [],
            "full_slots": [],
        }
        if not enabled:
            return out
        passed_now = _passed(con, epoch, now)
        held_now = _reserved_held(con, epoch, now)
        out["current_slot"] = {
            "slot": now,
            "passed": passed_now,
            "reserved_held": held_now,
            "used": passed_now + held_now,
            "remaining": max(0, limit - passed_now - held_now),
            "limit": limit,
        }
        rows = con.execute(
            "SELECT reservation_id, slot, status, created_sim, redeemed_sim "
            "FROM rl_reservations WHERE epoch=? "
            "ORDER BY slot ASC, reservation_id ASC LIMIT 100",
            (epoch,),
        ).fetchall()
        out["reservations"] = [dict(r) for r in rows]
        runs = con.execute(
            "SELECT start_slot, end_slot FROM rl_full_runs WHERE epoch=? ORDER BY start_slot",
            (epoch,),
        ).fetchall()
        out["full_slots"] = [
            {"start": int(r["start_slot"]), "end": int(r["end_slot"])} for r in runs
        ]
        return out


def set_enabled(enabled: bool) -> dict[str, Any]:
    """切换开关。关闭即作废本轮预约（旧轮次数据保留备查，epoch 递增开新一轮）。"""
    with db.session() as con:
        con.execute("BEGIN IMMEDIATE")
        cfg = _config_row(con)
        now = db.sim_now(con)
        want = 1 if enabled else 0
        if int(cfg["enabled"]) == want:
            con.execute("COMMIT")
            return {"enabled": bool(want), "epoch": int(cfg["epoch"]), "changed": False}

        if want:
            new_epoch = int(cfg["epoch"]) + 1
            con.execute(
                "UPDATE rl_config SET enabled=1, epoch=?, updated_sim=?, updated_real=? WHERE id=1",
                (new_epoch, now, time.time()),
            )
            db.log_event(
                con, "ratelimit_on",
                f"/api/catalog 限速开启（第 {new_epoch} 轮）：每模拟秒最多 "
                f"{config.CATALOG_RATE_PER_SECOND} 次",
                level="warn",
            )
            con.execute("COMMIT")
            return {"enabled": True, "epoch": new_epoch, "changed": True}

        # 关闭：作废本轮未兑现预约，清空本轮计数 / 满额区间
        epoch = int(cfg["epoch"])
        con.execute(
            "UPDATE rl_reservations SET status='cancelled', updated_real=? "
            "WHERE epoch=? AND status='reserved'",
            (time.time(), epoch),
        )
        con.execute("DELETE FROM rl_passed WHERE epoch=?", (epoch,))
        con.execute("DELETE FROM rl_full_runs WHERE epoch=?", (epoch,))
        new_epoch = epoch + 1
        con.execute(
            "UPDATE rl_config SET enabled=0, epoch=?, updated_sim=?, updated_real=? WHERE id=1",
            (new_epoch, now, time.time()),
        )
        db.log_event(
            con, "ratelimit_off",
            f"/api/catalog 限速关闭：第 {epoch} 轮预约全部作废，下一轮 epoch={new_epoch}",
            level="warn",
        )
        con.execute("COMMIT")
        return {"enabled": False, "epoch": new_epoch, "changed": True}


def admit_normal(earliest_retry: int | None = None) -> dict[str, Any]:
    """普通 GET（不带预约号）的准入。

    有余量 → 放行并累加该秒已放行数；无余量 → 创建预约并抛 429。
    """
    with db.session() as con:
        con.execute("BEGIN IMMEDIATE")
        cfg = _config_row(con)
        if not int(cfg["enabled"]):
            con.execute("COMMIT")
            return {"decision": "passed", "reason": "limiter_disabled"}
        epoch, now = int(cfg["epoch"]), db.sim_now(con)
        _expire_stale(con, epoch, now)
        limit = config.CATALOG_RATE_PER_SECOND
        used = _used(con, epoch, now)
        if used < limit:
            con.execute(
                "INSERT INTO rl_passed(epoch, slot, passed) VALUES(?, ?, 1) "
                "ON CONFLICT(epoch, slot) DO UPDATE SET passed=passed+1",
                (epoch, now),
            )
            if used + 1 >= limit:
                _mark_full(con, epoch, now)
            con.execute("COMMIT")
            return {
                "decision": "passed",
                "slot": now,
                "remaining": max(0, limit - used - 1),
                "limit": limit,
            }

        # 余量耗尽：预约到下一秒起最早有空的一秒（可指定最早重试时刻）
        not_before = now + 1
        if earliest_retry is not None:
            if int(earliest_retry) < now + 1:
                con.execute("COMMIT")
                raise RateLimitError(
                    400, "invalid_earliest_retry",
                    f"最早重试时刻必须晚于当前模拟秒 t={now}（即 ≥ {now + 1}）",
                )
            not_before = int(earliest_retry)
        slot = _earliest_free_slot(con, epoch, now, not_before)
        rid = _create_reservation(con, epoch, slot, now)
        db.log_event(
            con, "ratelimit_reserved",
            f"/api/catalog 当前秒 t={now} 已满（{used}/{limit}），预约号 {rid} 落在 t={slot}"
            + (f"，请求指定最早重试 t={not_before}" if earliest_retry is not None else ""),
            level="deny",
        )
        con.execute("COMMIT")
        raise RateLimitError(
            429, "rate_limited",
            f"当前模拟秒 t={now} 放行名额已用尽（{limit} 次/秒），"
            f"已预约 t={slot}，请凭预约号 {rid} 在该秒重试",
            extra={
                "reservation_id": rid,
                "slot": slot,
                "sim_now": now,
                "retry_after": slot - now,
                "limit": limit,
            },
        )


def _create_reservation(con, epoch: int, slot: int, now: int) -> str:
    rid = f"{RESV_PREFIX}-{db.new_id()}"
    con.execute(
        "INSERT INTO rl_reservations(reservation_id, epoch, slot, status, "
        "created_sim, updated_real) VALUES(?, ?, ?, 'reserved', ?, ?)",
        (rid, epoch, slot, now, time.time()),
    )
    # 预约即预占：若该秒因此满额，登记进满额区间（未来秒也只在真正被占满时才有行）
    if _used(con, epoch, slot) >= config.CATALOG_RATE_PER_SECOND:
        _mark_full(con, epoch, slot)
    return rid


def admit_reservation(reservation_id: str) -> dict[str, Any]:
    """凭号重试。约定秒未到 / 已过 / 已用 / 不存在 / 轮次作废都拒绝。"""
    rid = (reservation_id or "").strip()
    if not rid:
        raise RateLimitError(400, "missing_reservation", "缺少预约号")
    with db.session() as con:
        con.execute("BEGIN IMMEDIATE")
        cfg = _config_row(con)
        if not int(cfg["enabled"]):
            con.execute("COMMIT")
            raise RateLimitError(
                409, "limiter_disabled",
                "限速已关闭，本轮预约已全部作废，无需凭号重试（直接普通请求即可）",
            )
        epoch, now = int(cfg["epoch"]), db.sim_now(con)
        row = con.execute(
            "SELECT * FROM rl_reservations WHERE reservation_id=?", (rid,)
        ).fetchone()
        if row is None:
            con.execute("COMMIT")
            raise RateLimitError(404, "reservation_not_found", f"预约号 {rid} 不存在")
        r = dict(row)
        if int(r["epoch"]) != epoch:
            con.execute("COMMIT")
            raise RateLimitError(
                409, "reservation_void",
                f"预约号 {rid} 属于已作废的第 {r['epoch']} 轮（当前第 {epoch} 轮）",
            )
        status = r["status"]
        slot = int(r["slot"])
        if status == "reserved" and slot < now:
            # 约定秒已过仍未兑现：惰性置过期
            con.execute(
                "UPDATE rl_reservations SET status='expired', updated_real=? "
                "WHERE reservation_id=? AND status='reserved'",
                (time.time(), rid),
            )
            status = "expired"
        if status == "expired":
            con.execute("COMMIT")
            raise RateLimitError(
                409, "reservation_expired",
                f"预约号 {rid} 约定的 t={slot} 已过（当前 t={now}），预约已失效",
                extra={"reservation_id": rid, "slot": slot, "sim_now": now},
            )
        if status in ("redeemed", "cancelled"):
            con.execute("COMMIT")
            raise RateLimitError(
                409, f"reservation_{status}",
                f"预约号 {rid} 已{'兑现' if status == 'redeemed' else '退号'}，不能重复使用",
                extra={"reservation_id": rid, "slot": slot, "sim_now": now},
            )
        # status == reserved
        if slot > now:
            con.execute("COMMIT")
            raise RateLimitError(
                409, "reservation_not_due",
                f"预约号 {rid} 约定 t={slot} 才能使用，当前 t={now}，尚未到时",
                extra={
                    "reservation_id": rid, "slot": slot, "sim_now": now,
                    "retry_after": slot - now,
                },
            )

        # 约定秒就是当前秒：条件 UPDATE 保证同号并发最多兑现一次
        cur = con.execute(
            "UPDATE rl_reservations SET status='redeemed', redeemed_sim=?, updated_real=? "
            "WHERE reservation_id=? AND status='reserved' AND slot=?",
            (now, time.time(), rid, now),
        )
        if cur.rowcount != 1:
            con.execute("COMMIT")
            raise RateLimitError(
                409, "reservation_consumed",
                f"预约号 {rid} 已被并发请求兑现，同号并发最多放行一次",
                extra={"reservation_id": rid, "slot": slot, "sim_now": now},
            )
        # 兑现计入该秒已放行（名额在预约时已预占，因此这里不会再导致超额）
        con.execute(
            "INSERT INTO rl_passed(epoch, slot, passed) VALUES(?, ?, 1) "
            "ON CONFLICT(epoch, slot) DO UPDATE SET passed=passed+1",
            (epoch, now),
        )
        if _used(con, epoch, now) >= config.CATALOG_RATE_PER_SECOND:
            _mark_full(con, epoch, now)
        db.log_event(
            con, "ratelimit_redeemed",
            f"预约号 {rid} 在约定秒 t={slot} 兑现放行", level="success",
        )
        con.execute("COMMIT")
        return {
            "decision": "passed",
            "redeemed": True,
            "reservation_id": rid,
            "slot": now,
            "limit": config.CATALOG_RATE_PER_SECOND,
        }


def cancel(reservation_id: str) -> dict[str, Any]:
    """退号：释放名额。未来秒的空位可被重新预约。"""
    rid = (reservation_id or "").strip()
    if not rid:
        raise RateLimitError(400, "missing_reservation", "缺少预约号")
    with db.session() as con:
        con.execute("BEGIN IMMEDIATE")
        cfg = _config_row(con)
        epoch, now = int(cfg["epoch"]), db.sim_now(con)
        row = con.execute(
            "SELECT * FROM rl_reservations WHERE reservation_id=?", (rid,)
        ).fetchone()
        if row is None:
            con.execute("COMMIT")
            raise RateLimitError(404, "reservation_not_found", f"预约号 {rid} 不存在")
        r = dict(row)
        if r["status"] != "reserved":
            con.execute("COMMIT")
            raise RateLimitError(
                409, f"reservation_{r['status']}",
                f"预约号 {rid} 当前状态为 {r['status']}，无法退号",
            )
        if int(r["epoch"]) != epoch or not int(cfg["enabled"]):
            # 轮次已作废：直接落状态，不释放任何名额（本轮数据本就失效）
            con.execute(
                "UPDATE rl_reservations SET status='cancelled', updated_real=? "
                "WHERE reservation_id=?",
                (time.time(), rid),
            )
            con.execute("COMMIT")
            return {"cancelled": rid, "released": False}

        slot = int(r["slot"])
        was_full = _used(con, epoch, slot) >= config.CATALOG_RATE_PER_SECOND
        con.execute(
            "UPDATE rl_reservations SET status='cancelled', updated_real=? "
            "WHERE reservation_id=? AND status='reserved'",
            (time.time(), rid),
        )
        released_future = slot > now
        if was_full:
            # 退号后该秒必然不再满额：从满额区间摘除（未来秒因此重新可预约）
            _release_slot(con, epoch, slot)
        db.log_event(
            con, "ratelimit_cancelled",
            f"预约号 {rid} 退号，释放 t={slot} 的名额"
            + ("（未来秒空位可重新预约）" if released_future else "（当前 / 已过秒）"),
            level="warn",
        )
        con.execute("COMMIT")
        return {
            "cancelled": rid,
            "released": True,
            "slot": slot,
            "future_slot": released_future,
        }
