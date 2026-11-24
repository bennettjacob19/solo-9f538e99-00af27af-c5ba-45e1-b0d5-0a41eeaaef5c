"""SQLite 持久化：模拟时钟、租约、fencing 序号、版本、回执、任务、事件。

Flask 进程与发布端子进程会并发访问同一个 SQLite 文件，因此：
* WAL + busy_timeout；
* 关键事务使用 ``BEGIN IMMEDIATE`` 立即拿写锁，保证并发申请只有一个成功；
* 每次操作新开短连接（连接不跨线程复用）。
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

-- 单行租约（id 恒为 1）
CREATE TABLE IF NOT EXISTS lease (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    holder      TEXT NOT NULL,
    token       INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,           -- 模拟时钟下的到期时刻
    updated_real REAL NOT NULL
);

-- 每个 worker “自认”的最近 token，页面刷新 / 重启后仍可保留旧令牌用于重试
CREATE TABLE IF NOT EXISTS worker_state (
    holder      TEXT PRIMARY KEY,
    last_token  INTEGER NOT NULL,
    updated_real REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    version      INTEGER PRIMARY KEY,
    request_id   TEXT NOT NULL UNIQUE,
    holder       TEXT NOT NULL,
    token        INTEGER NOT NULL,
    config_json  TEXT NOT NULL,
    applied      INTEGER NOT NULL DEFAULT 0, -- 文件切换是否已完成
    created_sim  INTEGER NOT NULL,
    created_real REAL NOT NULL
);

-- 幂等回执：同号重发直接返回这里的原始回执
CREATE TABLE IF NOT EXISTS receipts (
    request_id   TEXT PRIMARY KEY,
    version      INTEGER NOT NULL,
    holder       TEXT NOT NULL,
    token        INTEGER NOT NULL,
    body_json    TEXT NOT NULL,
    recovered    INTEGER NOT NULL DEFAULT 0,
    created_sim  INTEGER NOT NULL,
    created_real REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS publish_runs (
    run_id       TEXT PRIMARY KEY,
    request_id   TEXT NOT NULL,
    holder       TEXT NOT NULL,
    token        INTEGER NOT NULL,
    disabled_json TEXT NOT NULL DEFAULT '[]',
    pause_at     TEXT NOT NULL DEFAULT 'none',
    pid          INTEGER,
    phase        TEXT NOT NULL DEFAULT 'queued',
    status       TEXT NOT NULL DEFAULT 'running', -- running|paused|done|rejected|failed|killed|interrupted
    reason_code  TEXT,
    reason_detail TEXT,
    bytes_total  INTEGER,
    bytes_written INTEGER,
    staging_path TEXT,
    log_path     TEXT,
    created_sim  INTEGER NOT NULL,
    started_real REAL NOT NULL,
    updated_real REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_request ON publish_runs(request_id);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_real     REAL NOT NULL,
    sim_now     INTEGER NOT NULL,
    level       TEXT NOT NULL DEFAULT 'info',  -- info|success|warn|error|deny
    kind        TEXT NOT NULL,
    holder      TEXT,
    request_id  TEXT,
    message     TEXT NOT NULL
);

-- ===========================================================================
-- /api/catalog 维护后重试限速（开关默认关闭；状态全部在 SQLite，可跨重启）
-- ===========================================================================

-- 单行配置（id 恒为 1）：enabled 开关 + 轮次 epoch（关闭即作废本轮预约，epoch+1）
CREATE TABLE IF NOT EXISTS rl_config (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    enabled     INTEGER NOT NULL DEFAULT 0,
    epoch       INTEGER NOT NULL DEFAULT 1,
    updated_sim INTEGER NOT NULL,
    updated_real REAL NOT NULL
);

-- 预约号：创建即预占 slot 秒的一个名额；每个预约号只能在约定秒兑现一次
CREATE TABLE IF NOT EXISTS rl_reservations (
    reservation_id TEXT PRIMARY KEY,
    epoch      INTEGER NOT NULL,          -- 所属轮次；开关关闭后旧轮次全部作废
    slot       INTEGER NOT NULL,          -- 约定的模拟秒
    status     TEXT NOT NULL,             -- reserved|redeemed|cancelled|expired
    created_sim INTEGER NOT NULL,
    redeemed_sim INTEGER,
    updated_real REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rl_resv_slot
    ON rl_reservations(epoch, slot, status);
CREATE INDEX IF NOT EXISTS idx_rl_resv_status
    ON rl_reservations(status, slot);

-- 已放行的普通请求计数：只在真实放行的那一秒建行，绝不给“未预约的未来秒”预建
CREATE TABLE IF NOT EXISTS rl_passed (
    epoch      INTEGER NOT NULL,
    slot       INTEGER NOT NULL,
    passed     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (epoch, slot)
);

-- 已满额的连续秒区间（run）：把“找最早空档”变成索引上的一次区间探测，
-- 读取的行数只与“命中的满额区间个数”有关（首个空档前至多 1 个 run 覆盖），
-- 不随连续满额的秒数线性增长；未预约 / 未满的未来秒不会有任何记录。
CREATE TABLE IF NOT EXISTS rl_full_runs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch      INTEGER NOT NULL,
    start_slot INTEGER NOT NULL,
    end_slot   INTEGER NOT NULL           -- 闭区间
);
CREATE INDEX IF NOT EXISTS idx_rl_runs_lookup
    ON rl_full_runs(epoch, start_slot, end_slot);
"""


def new_id() -> str:
    return uuid.uuid4().hex[:12]


def connect() -> sqlite3.Connection:
    config.ensure_dirs()
    con = sqlite3.connect(str(config.DB_PATH), timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA foreign_keys=ON")
    return con


@contextmanager
def session() -> Iterator[sqlite3.Connection]:
    con = connect()
    try:
        yield con
    finally:
        con.close()


def init_db() -> None:
    config.ensure_dirs()
    with session() as con:
        con.executescript(SCHEMA)
        con.execute(
            "INSERT OR IGNORE INTO kv(k, v) VALUES('sim_now', ?)",
            (str(config.START_NOW),),
        )
        con.execute("INSERT OR IGNORE INTO kv(k, v) VALUES('next_token', '1')")
        # none | disk_full | io_error ：下一次正文写入时消费一次
        con.execute("INSERT OR IGNORE INTO kv(k, v) VALUES('fault_next_write', '')")
        # 限速开关默认关闭
        con.execute(
            "INSERT OR IGNORE INTO rl_config(id, enabled, epoch, updated_sim, updated_real) "
            "VALUES(1, 0, 1, ?, ?)",
            (config.START_NOW, time.time()),
        )


# ---------- kv / clock ----------

def kv_get(con: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = con.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    return row["v"] if row is not None else default


def kv_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO kv(k, v) VALUES(?, ?) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )


def sim_now(con: sqlite3.Connection) -> int:
    return int(kv_get(con, "sim_now", str(config.START_NOW)))


def advance_clock(seconds: int) -> int:
    with session() as con:
        now = sim_now(con) + int(seconds)
        kv_set(con, "sim_now", str(now))
        log_event(con, "clock", f"模拟时钟推进 {seconds}s → t={now}")
        return now


# ---------- events ----------

def log_event(
    con: sqlite3.Connection,
    kind: str,
    message: str,
    level: str = "info",
    holder: str | None = None,
    request_id: str | None = None,
) -> None:
    con.execute(
        "INSERT INTO events(ts_real, sim_now, level, kind, holder, request_id, message) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (time.time(), sim_now(con), level, kind, holder, request_id, message),
    )


def recent_events(limit: int = 40) -> list[dict[str, Any]]:
    with session() as con:
        rows = con.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


# ---------- serialization helpers ----------

def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def receipt_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        row = dict(row)
    body = json.loads(row["body_json"])
    return body
