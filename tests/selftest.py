#!/usr/bin/env python3
"""端到端实测：启动真实 Flask 服务 + 真实 publisher 子进程，覆盖：

1. 网关 200/503/404 与 X-Config-Version；
2. 并发申请租约恰好一个成功；续租 / 错误 token / 过期；
3. 正常发布并让停用路由 503；
4. 接管：旧进程在两个“生效前”暂停点挂起 → 时钟过期 → B 连续发布两版 →
   旧进程恢复被 fencing 拒绝，配置 / 版号不变；
5. 磁盘槽位约束 + “已删除但仍打开”幽灵文件 /proc 核算；
6. 生效未回包：同号重发回放原回执、不升版（原进程仍暂停）；
7. 强杀发布端 → recover 认回租约 / token；整服务重启后状态仍在并可回放；
8. 写入故障（ENOSPC / EIO）不影响网关按最近完整配置响应；
9. /api/catalog 重试限速：开关默认关闭、2 次/模拟秒、429 预约号 + Retry-After、
   凭号重试 / 退号补位 / 同号并发只放一次 / 停用 503 不发号 / 关闭作废、
   连续满秒下找空档 O(1) 且不预建未来秒。

只用标准库（urllib + threading + sqlite3），由当前解释器启动 app.py（需要 Flask）。
"""
from __future__ import annotations

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS_COUNT = 0
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS_COUNT
    if cond:
        PASS_COUNT += 1
        print(f"  \033[32mPASS\033[0m {name}")
    else:
        FAILURES.append(name + (f" :: {detail}" if detail else ""))
        print(f"  \033[31mFAIL\033[0m {name} {detail}")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Client:
    def __init__(self, base: str):
        self.base = base

    def call(self, method: str, path: str, body=None):
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode()
                return resp.status, dict(resp.headers), json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"raw": raw}
            return e.code, dict(e.headers), parsed

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, body=None):
        return self.call("POST", path, body or {})

    def state(self):
        return self.get("/api/state")[2]

    def wait_run(self, run_id, terminal_or_paused=True, timeout=25.0):
        """等到 run 进入 paused 或终态；返回 run dict。"""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            _, _, d = self.get(f"/api/runs/{run_id}")
            last = d["run"]
            if last["status"] == "paused":
                return last
            if last["status"] in ("done", "rejected", "failed", "killed", "interrupted"):
                return last
            time.sleep(0.1)
        raise AssertionError(f"wait_run timeout {run_id}: {last}")

    def wait_status(self, run_id, statuses, timeout=25.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            _, _, d = self.get(f"/api/runs/{run_id}")
            last = d["run"]
            if last["status"] in statuses:
                return last
            time.sleep(0.1)
        raise AssertionError(f"wait_status {statuses} timeout: {last}")


class Server:
    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="drill-selftest-")
        self.data = Path(self.tmp) / "config"
        self.state_dir = Path(self.tmp) / "state"
        self.data.mkdir()
        self.state_dir.mkdir()
        self.port = free_port()
        self.log = open(Path(self.tmp) / "server.log", "ab")
        env = dict(os.environ)
        env.update({
            "DRILL_DATA_DIR": str(self.data),
            "DRILL_STATE_DIR": str(self.state_dir),
            "DRILL_PORT": str(self.port),
            "DRILL_LEASE_TTL": "30",
            # 保留外部已有的 PYTHONPATH（例如沙箱里 Flask 装在独立目录）
            "PYTHONPATH": str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""),
        })
        self.proc = subprocess.Popen(
            [sys.executable, str(ROOT / "app.py")],
            cwd=str(ROOT), env=env, stdout=self.log,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        self.c = Client(f"http://127.0.0.1:{self.port}")
        self._wait_health()

    def _wait_health(self):
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("server exited early; log:\n" + self.log_text())
            try:
                s, _, d = self.c.get("/api/health")
                if s == 200:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise RuntimeError("server health timeout; log:\n" + self.log_text())

    def wait_run(self, run_id, timeout=25.0):
        return self.c.wait_run(run_id, timeout=timeout)

    def wait_status(self, run_id, statuses, timeout=25.0):
        return self.c.wait_status(run_id, statuses, timeout=timeout)

    def log_text(self):
        self.log.flush()
        try:
            return (Path(self.tmp) / "server.log").read_text(errors="replace")[-4000:]
        except Exception:
            return "(no log)"

    def hard_restart(self):
        """模拟整机 / 容器级硬重启：SIGKILL 服务进程组后用同一卷重启。"""
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=5)
        time.sleep(0.3)
        self.log.close()
        self.log = open(Path(self.tmp) / "server.log", "ab")
        env = dict(os.environ)
        env.update({
            "DRILL_DATA_DIR": str(self.data),
            "DRILL_STATE_DIR": str(self.state_dir),
            "DRILL_PORT": str(self.port),
            "DRILL_LEASE_TTL": "30",
            "PYTHONPATH": str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""),
        })
        self.proc = subprocess.Popen(
            [sys.executable, str(ROOT / "app.py")],
            cwd=str(ROOT), env=env, stdout=self.log,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        self._wait_health()

    def stop(self):
        # 先尝试通过 API 清掉所有 publisher（含 SIGCONT + KILL）
        try:
            self.c.post("/api/admin/reset", {})
            time.sleep(0.2)
        except Exception:
            pass
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
            self.proc.wait(timeout=5)
        except Exception:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except Exception:
                pass
        self.log.close()


def reset(s: Server):
    st, _, d = s.c.post("/api/admin/reset", {})
    assert st == 200, d
    # 等模拟时钟回到 1000
    for _ in range(50):
        if s.c.state()["sim_now"] == 1000:
            return
        time.sleep(0.05)


def acquire(s: Server, holder, token_expected=None):
    st, _, d = s.c.post("/api/lease/acquire", {"holder": holder})
    return st, d


def publish(s: Server, holder, token, req, disabled, pause="none"):
    st, _, d = s.c.post("/api/publish", {
        "holder": holder, "token": token, "request_id": req,
        "disabled": disabled, "pause_at": pause,
    })
    return st, d


# ---------------------------------------------------------------------------
# 场景
# ---------------------------------------------------------------------------

def test_gateway_seed(s: Server):
    print("\n[1] 网关播种状态：200 / 404 / 版本头")
    reset(s)
    st, hdr, _ = s.c.get("/gw/health")
    check("enabled route /gw/health -> 200", st == 200, str(st))
    check("X-Config-Version=0", hdr.get("X-Config-Version") == "0", str(hdr.get("X-Config-Version")))
    st, _, _ = s.c.get("/gw/nope-not-exist")
    check("unknown route -> 404", st == 404, str(st))
    st, _, d = s.c.get("/api/state")
    check("disk slot invariant at seed", d["disk"]["slot_invariant_ok"], json.dumps(d["disk"].get("unexpected_files")))


def test_concurrent_acquire(s: Server):
    print("\n[2] 并发申请：8 个不同 worker 同时抢，恰好一个成功")
    reset(s)
    results = []
    barrier = threading.Barrier(8)

    def worker(i):
        barrier.wait()
        results.append(acquire(s, f"race-{i}"))

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    oks = [(st, d) for st, d in results if st == 200]
    denied = [r for r in results if r[0] == 409]
    check("exactly one acquire succeeds", len(oks) == 1, f"oks={len(oks)}")
    check("other seven get 409", len(denied) == 7, f"denied={len(denied)}")
    if oks:
        check("winner gets token=1 (first lease)", oks[0][1]["lease"]["token"] == 1)
    stt, _, _ = s.c.post("/api/clock/advance", {"seconds": 31})
    check("clock advance ok", stt == 200)

    print("  -- 全新状态下 A/B Promise 式同时申请")
    reset(s)
    out = {}

    def ab(h):
        out[h] = acquire(s, h)

    ta, tb = threading.Thread(target=ab, args=("A",)), threading.Thread(target=ab, args=("B",))
    ta.start(); tb.start(); ta.join(); tb.join()
    a_ok = out["A"][0] == 200
    b_ok = out["B"][0] == 200
    check("A/B race exactly one winner", a_ok != b_ok, f"A={out['A'][0]} B={out['B'][0]}")
    winner = "A" if a_ok else "B"
    check("winner token is 1", out[winner][1]["lease"]["token"] == 1)
    loser = "B" if a_ok else "A"
    check("loser 409 has held_by_other reason",
          out[loser][1].get("error_code") == "held_by_other" and winner in out[loser][1].get("error", ""),
          json.dumps(out[loser][1], ensure_ascii=False)[:200])


def test_renew(s: Server):
    print("\n[3] 续租：正确 token 成功；错误 token / 过期被拒")
    reset(s)
    st, d = acquire(s, "A")
    check("A granted token 1", st == 200 and d["lease"]["token"] == 1)
    s.c.post("/api/clock/advance", {"seconds": 20})  # t=1020, 剩 10
    st, _, d = s.c.post("/api/lease/renew", {"holder": "A", "token": 999})
    check("renew with wrong token -> 409 token_mismatch",
          st == 409 and d.get("error_code") == "token_mismatch", str(d)[:200])
    st, _, d = s.c.post("/api/lease/renew", {"holder": "A", "token": 1})
    check("renew with right token -> 200 and new expiry 1050",
          st == 200 and d["lease"]["expires_at"] == 1050, str(d)[:200])
    s.c.post("/api/clock/advance", {"seconds": 30})  # t=1050 临界过期
    st, _, d = s.c.post("/api/lease/renew", {"holder": "A", "token": 1})
    check("renew at deadline -> 409 lease_expired",
          st == 409 and d.get("error_code") == "lease_expired", str(d)[:200])
    st, d = acquire(s, "A")
    check("re-acquire after expiry bumps token to 2",
          st == 200 and d["lease"]["token"] == 2, str(d)[:200])


def test_publish_and_503(s: Server):
    print("\n[4] 正常发布 v1，停用 /api/checkout -> 503")
    reset(s)
    acquire(s, "A")
    st, d = publish(s, "A", 1, "req-normal-1", ["/api/checkout"])
    check("publish accepted 202", st == 202 and d.get("run_id"), str(d)[:200])
    run = s.wait_status(d["run_id"], {"done"})
    check("run done", run["status"] == "done", run.get("reason_detail", ""))
    st, hdr, _ = s.c.get("/gw/api/checkout")
    check("disabled route -> 503", st == 503, str(st))
    check("503 carries config version 1", hdr.get("X-Config-Version") == "1", str(hdr))
    st, _, body = s.c.get("/gw/health")
    check("enabled route still 200", st == 200)
    st, _, rcpt = s.c.get("/api/receipts/req-normal-1")
    check("receipt stored with version 1", st == 200 and rcpt["receipt"]["version"] == 1)


def _takeover(s: Server, pause_point: str):
    print(f"\n[5.{pause_point}] 接管剧本（暂停点 {pause_point}）")
    reset(s)
    acquire(s, "A")
    req_a = f"req-a-{pause_point}"
    st, d = publish(s, "A", 1, req_a, ["/api/checkout"], pause=pause_point)
    check(f"[{pause_point}] A publish accepted", st == 202, str(d)[:200])
    rid_a = d["run_id"]
    run = s.wait_run(rid_a)
    check(f"[{pause_point}] A paused at {pause_point}",
          run["status"] == "paused" and run["phase"] == pause_point,
          f"{run['status']}/{run['phase']}")
    check(f"[{pause_point}] real OS process state T (SIGSTOP)",
          run.get("proc_state") == "T", str(run.get("proc_state")))

    # A 暂停期间（B 尚未接管 / 发布），网关必须仍按最近完整配置 v0 正常响应
    st, hdr, _ = s.c.get("/gw/api/checkout")
    check(f"[{pause_point}] gateway still serves last complete v0 while publisher paused",
          st == 200 and hdr.get("X-Config-Version") == "0", f"{st}/{hdr.get('X-Config-Version')}")

    if pause_point == "stage_half":
        sp = s.data / "staging.json"
        check("staging.json exists while half-written", sp.exists())
        if sp.exists():
            check("staging file is partial on disk",
                  0 < sp.stat().st_size < (run["bytes_total"] or 1),
                  f"{sp.stat().st_size} vs total {run['bytes_total']}")
        check("half bytes reported", run["bytes_written"] and run["bytes_written"] < run["bytes_total"])
    else:
        check("staging fully written before switch pause",
              run["bytes_written"] == run["bytes_total"],
              f"{run['bytes_written']}/{run['bytes_total']}")

    # 时钟推进到租约过期
    s.c.post("/api/clock/advance", {"seconds": 31})
    st, d = acquire(s, "B")
    check(f"[{pause_point}] B takeover granted token 2",
          st == 200 and d["lease"]["token"] == 2, str(d)[:200])

    # B 连续发布两版（A 仍暂停）
    st, d1 = publish(s, "B", 2, f"req-b1-{pause_point}", ["/api/checkout", "/admin/reload"])
    r1 = s.wait_status(d1["run_id"], {"done"})
    check("B first publish done -> v1", r1["status"] == "done", r1.get("reason_detail", ""))
    st, hdr, _ = s.c.get("/gw/api/checkout")
    check("after B#1 checkout 503 v1", st == 503 and hdr.get("X-Config-Version") == "1", f"{st}/{hdr.get('X-Config-Version')}")
    st, hdr, _ = s.c.get("/gw/admin/reload")
    check("after B#1 admin/reload 503", st == 503 and hdr.get("X-Config-Version") == "1")

    state = s.c.state()
    ghost_a = [g for g in state["disk"]["ghost_files"] if g.get("request_id") == req_a]
    if pause_point == "stage_half":
        # 只有“写一半”暂停点仍持有打开的 fd；B unlink 后成为已删但仍打开的幽灵 inode
        check("A's unlinked open inode counted as deleted-open ghost",
              len(ghost_a) == 1 and ghost_a[0]["deleted_open"] and ghost_a[0]["pid"] == run["pid"],
              json.dumps(state["disk"]["ghost_files"], ensure_ascii=False)[:300])
    else:
        # before_switch 时正文已 fsync 并正常 close，unlink 不产生打开句柄
        check("no deleted-open ghost once A closed its fd before switch",
              len(ghost_a) == 0, json.dumps(state["disk"]["ghost_files"], ensure_ascii=False)[:300])

    st, d2 = publish(s, "B", 2, f"req-b2-{pause_point}", ["/api/checkout"])
    r2 = s.wait_status(d2["run_id"], {"done"})
    check("B second publish done -> v2", r2["status"] == "done", r2.get("reason_detail", ""))
    st, hdr, _ = s.c.get("/gw/admin/reload")
    check("after B#2 admin/reload re-enabled 200 v2",
          st == 200 and hdr.get("X-Config-Version") == "2", f"{st}/{hdr.get('X-Config-Version')}")
    st, hdr, _ = s.c.get("/gw/api/checkout")
    check("after B#2 checkout still 503 v2", st == 503 and hdr.get("X-Config-Version") == "2")

    # 恢复旧进程：fencing 必须拒绝
    st, _, d = s.c.post(f"/api/runs/{rid_a}/resume", {})
    check("resume signal accepted", st == 200, str(d)[:200])
    final = s.wait_status(rid_a, {"rejected", "done", "failed"})
    check("stale A rejected by fencing",
          final["status"] == "rejected" and "token" in (final["reason_detail"] or ""),
          f"{final['status']}: {final.get('reason_detail')}")

    state = s.c.state()
    check("config version unchanged at 2 after stale resume",
          state["config"]["version"] == 2, str(state["config"]["version"]))
    check("config disabled equals B#2 list",
          state["config"]["disabled"] == ["/api/checkout"], str(state["config"]["disabled"]))
    reqs = {v["request_id"] for v in state["versions"]}
    check("A request never created a version", req_a not in reqs, str(reqs))
    check("two B versions present",
          {v["version"] for v in state["versions"]} == {1, 2},
          str([(v["version"], v["request_id"]) for v in state["versions"]]))
    st, _, _ = s.c.get(f"/api/receipts/{req_a}")
    check("A has no receipt", st == 404)

    # A 进程结束后幽灵消失；磁盘只剩 current.json 一个正文
    deadline = time.time() + 5
    disk = state["disk"]
    while time.time() < deadline:
        disk = s.c.state()["disk"]
        if not any(g.get("request_id") == req_a for g in disk["ghost_files"]):
            break
        time.sleep(0.1)
    check("ghost released after A exits",
          not any(g.get("request_id") == req_a for g in disk["ghost_files"]),
          json.dumps(disk["ghost_files"])[:200])
    check("slot invariant holds", disk["slot_invariant_ok"], str(disk.get("unexpected_files")))
    check("only current.json body remains",
          [f["name"] for f in disk["named_files"] if f["is_body"]] == ["current.json"],
          str([f["name"] for f in disk["named_files"]]))


def test_idempotent_and_restart(s: Server):
    print("\n[6] 生效未回包：同号重发回放 + 强杀 + recover + 整服务重启")
    reset(s)
    acquire(s, "A")
    req = "req-idem-1"
    st, d = publish(s, "A", 1, req, ["/api/checkout"], pause="effective_no_response")
    check("accepted", st == 202)
    run = s.wait_run(d["run_id"])
    check("paused at effective_no_response", run["status"] == "paused" and run["phase"] == "effective_no_response")

    st, _, rcpt = s.c.get(f"/api/receipts/{req}")
    check("receipt already durable before response", st == 200 and rcpt["receipt"]["version"] == 1)

    # 原进程仍暂停时同号重发
    st, d2 = publish(s, "A", 1, req, ["/api/checkout"], pause="effective_no_response")
    check("same-id resend -> 200 replayed", st == 200 and d2.get("replayed") is True, str(d2)[:200])
    check("replayed receipt same version 1", d2["receipt"]["version"] == 1)
    versions = s.c.state()["versions"]
    check("still exactly one version", [v["version"] for v in versions] == [1], str(len(versions)))

    # 原进程恢复后正常 done，仍然不升版
    s.c.post(f"/api/runs/{d['run_id']}/resume", {})
    fin = s.wait_status(d["run_id"], {"done"})
    check("original run finishes done", fin["status"] == "done")
    check("version still 1 after original resumes", s.c.state()["config"]["version"] == 1)

    # 再来一个生效暂停的请求，强杀发布端，recover 认回租约 / token
    req_kill = "req-kill-1"
    st, dk = publish(s, "A", 1, req_kill, ["/api/checkout", "/api/login"], pause="effective_no_response")
    runk = s.wait_run(dk["run_id"])
    check("second run paused effective", runk["status"] == "paused")
    st, _, _ = s.c.post(f"/api/runs/{dk['run_id']}/kill", {})
    check("kill api 200", st == 200)
    time.sleep(0.3)
    st, _, rec = s.c.post("/api/publisher/recover", {})
    lic = rec["report"]["lease"]
    check("recover sees lease holder A token 1",
          st == 200 and lic and lic["holder"] == "A" and lic["token"] == 1, json.dumps(rec)[:300])
    st, d3 = publish(s, "A", 1, req_kill, [], pause="none")
    check("killed effective request resend replays receipt v2",
          st == 200 and d3.get("replayed") and d3["receipt"]["version"] == 2, str(d3)[:200])
    check("no version bump from replay", s.c.state()["config"]["version"] == 2)

    # 整服务硬重启（同一本地卷 + SQLite）：租约 / token / 回执都还在
    req_restart = "req-restart-1"
    st, dr = publish(s, "A", 1, req_restart, ["/health"], pause="effective_no_response")
    s.wait_run(dr["run_id"])
    st, _, _ = s.c.post(f"/api/runs/{dr['run_id']}/kill", {})
    assert st == 200
    print("  -- hard restart flask service --")
    s.hard_restart()
    st, _, h = s.c.get("/api/health")
    check("server healthy after restart", st == 200)
    state = s.c.state()
    check("lease/token survived restart",
          state["lease"] and state["lease"]["holder"] == "A" and state["lease"]["token"] == 1,
          json.dumps(state["lease"]))
    check("config still v3 after restart (req-restart was effective before kill)",
          state["config"]["version"] == 3, str(state["config"]["version"]))
    st, hdr, _ = s.c.get("/gw/health")
    check("gateway keeps serving effective v3 (disabled /health -> 503) after restart",
          st == 503 and hdr.get("X-Config-Version") == "3", f"{st}/{hdr.get('X-Config-Version')}")
    st, hdr, _ = s.c.get("/gw/api/catalog")
    check("gateway enabled route 200 at v3 after restart",
          st == 200 and hdr.get("X-Config-Version") == "3", f"{st}/{hdr.get('X-Config-Version')}")
    st, _, rcpt = s.c.get(f"/api/receipts/{req_restart}")
    check("effective receipt survived restart (v3)", st == 200 and rcpt["receipt"]["version"] == 3)
    st, d4 = publish(s, "A", 1, req_restart, [], pause="none")
    check("same-id resend after restart returns original receipt",
          st == 200 and d4.get("replayed") and d4["receipt"]["version"] == 3, str(d4)[:200])
    check("still max version 3", max(v["version"] for v in s.c.state()["versions"]) == 3)
    recovered_events = [e for e in state["events"] if e["kind"] == "publisher_recover"]
    check("startup recovery event recorded", len(recovered_events) >= 1)


def test_write_failure(s: Server):
    print("\n[7] 写入故障：ENOSPC / EIO 时网关不受影响，之后可正常发布")
    reset(s)
    acquire(s, "A")
    s.c.post("/api/fault", {"mode": "disk_full"})
    st, d = publish(s, "A", 1, "req-fail-enospc", ["/api/checkout"])
    run = s.wait_status(d["run_id"], {"failed"})
    check("ENOSPC run failed with write_failed",
          run["status"] == "failed" and "write_failed" in (run["reason_code"] or "")
          and "ENOSPC" in (run["reason_detail"] or ""),
          f"{run['reason_code']} {run['reason_detail']}")
    st, hdr, _ = s.c.get("/gw/health")
    check("gateway still 200 on seed v0", st == 200 and hdr.get("X-Config-Version") == "0")
    disk = s.c.state()["disk"]
    check("current.json + at most one staging body after failure",
          disk["slot_invariant_ok"] and disk["body_file_count"] <= 2,
          str([f["name"] for f in disk["named_files"]]))

    s.c.post("/api/fault", {"mode": "io_error"})
    st, d = publish(s, "A", 1, "req-fail-eio", ["/api/checkout"])
    run = s.wait_status(d["run_id"], {"failed"})
    check("EIO run failed", run["status"] == "failed" and "EIO" in (run["reason_detail"] or ""))
    check("gateway still v0 after second failure",
          s.c.get("/gw/health")[1].get("X-Config-Version") == "0")

    st, d = publish(s, "A", 1, "req-ok-after-fail", ["/api/checkout"])
    run = s.wait_status(d["run_id"], {"done"})
    check("publish succeeds after faults consumed", run["status"] == "done", run.get("reason_detail", ""))
    st, hdr, _ = s.c.get("/gw/api/checkout")
    check("now checkout 503 at v1", st == 503 and hdr.get("X-Config-Version") == "1")
    check("fault arm consumed (cleared)", s.c.state()["fault_next_write"] == "")


def test_catalog_rate_limit(s: Server):
    print("\n[8] /api/catalog 重试限速：开关 / 429 预约 / 凭号 / 退号 / 并发 / 503 / 作废 / O(1) 找空档")
    reset(s)

    def rl_state():
        return s.c.get("/api/ratelimit/state")[2]["ratelimit"]

    def catalog_get(qs="", headers=None):
        url = "/gw/api/catalog" + qs
        # Client.call 不支持自定义头，这里直接走 urllib
        req = urllib.request.Request(s.c.base + url, method="GET")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status, dict(resp.headers), json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), json.loads(e.read().decode())

    # ---- 8.0 默认关闭：行为与普通网关一致，state 里 enabled=False ----
    rl = rl_state()
    check("rate limit disabled by default", rl["enabled"] is False and rl["rate_per_second"] == 2)
    st, _, _ = catalog_get()
    check("catalog 200 while limiter off", st == 200)
    st, _, d = s.c.post("/api/ratelimit/config", {"enabled": True})
    check("enable returns new epoch", st == 200 and d["enabled"] is True and d["epoch"] >= 2)
    rl = rl_state()
    check("state shows enabled and current slot",
          rl["enabled"] is True and rl["current_slot"]["remaining"] == 2
          and rl["current_slot"]["slot"] == 1000)

    # ---- 8.1 同秒 2 次放行，第 3 次 429，带预约号与 Retry-After ----
    st1, _, b1 = catalog_get()
    st2, h2, b2 = catalog_get()
    st3, h3, b3 = catalog_get()
    check("first two requests pass", st1 == 200 and st2 == 200)
    check("third request 429", st3 == 429, str(st3))
    check("429 carries reservation id and Retry-After",
          b3.get("reservation_id", "").startswith("RSV-") and h3.get("Retry-After") == "1",
          json.dumps(b3, ensure_ascii=False)[:200])
    check("429 books next simulated second", b3["slot"] == 1001 and b3["sim_now"] == 1000)
    check("passed payload echoes rate_limit info",
          b1.get("rate_limit", {}).get("decision") == "passed")
    rid1, slot1 = b3["reservation_id"], b3["slot"]
    rl = rl_state()
    check("current slot shows 2/2 used and zero remaining",
          rl["current_slot"]["used"] == 2 and rl["current_slot"]["remaining"] == 0)
    check("reservation visible in state snapshot",
          any(r["reservation_id"] == rid1 and r["status"] == "reserved" for r in rl["reservations"]))

    # ---- 8.2 未到时凭号 -> 409；推进到约定秒 -> 200 兑现；同号再试 -> 409 ----
    st, _, b = catalog_get(f"?reservation_id={rid1}")
    check("redeem before due -> 409 reservation_not_due",
          st == 409 and b.get("error") == "reservation_not_due", str(b)[:160])
    s.c.post("/api/clock/advance", {"seconds": slot1 - 1000})
    st, hdr, b = catalog_get(f"?reservation_id={rid1}")
    check("redeem on due second -> 200 redeemed",
          st == 200 and b.get("rate_limit", {}).get("redeemed") is True
          and b["rate_limit"]["reservation_id"] == rid1, str(b)[:160])
    st, _, b = catalog_get(f"?reservation_id={rid1}")
    check("same id second use -> 409 reservation_redeemed",
          st == 409 and b.get("error") == "reservation_redeemed", str(b)[:160])
    # 预约号也支持请求头传入（未到 / 未知号路径）
    st, _, b = catalog_get(headers={"X-Reservation-Id": "RSV-not-exist"})
    check("unknown reservation -> 404", st == 404 and b.get("error") == "reservation_not_found")
    st, _, b = catalog_get(headers={"X-Reservation-Id": rid1})
    check("header-based redeem also blocked after use", st == 409)

    # ---- 8.3 同号并发：6 个并发请求恰好兑现一次 ----
    # 先把本秒第二个名额用普通请求占掉，再制造一个新预约
    st, _, _ = catalog_get()  # 本秒第 2 次放行（第 1 次是上面的兑现）
    st, _, b = catalog_get()
    check("second full -> 429 again", st == 429 and b["slot"] == 1002)
    rid2 = b["reservation_id"]
    s.c.post("/api/clock/advance", {"seconds": 1})  # t=1002
    results2: list[tuple[int, dict]] = []
    barrier = threading.Barrier(6)

    def concurrent_redeem():
        barrier.wait()
        results2.append(catalog_get(f"?reservation_id={rid2}"))

    ts = [threading.Thread(target=concurrent_redeem) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    redeemed = [1 for st, _, b in results2
                if st == 200 and b.get("rate_limit", {}).get("redeemed")]
    rejected = [1 for st, _, _ in results2 if st == 409]
    check("concurrent same-id redeem: exactly one passes",
          len(redeemed) == 1, f"redeemed={len(redeemed)}")
    check("concurrent same-id redeem: other five get 409",
          len(rejected) == 5, f"rejected={len(rejected)}")

    # ---- 8.4 退号补位：未来秒退号后空位可被重新预约 ----
    # t=1002：并发兑现只放了 1 个，还剩 1 个普通名额，先吃掉它
    st, _, b0 = catalog_get()
    check("one ordinary slot remains after single concurrent redeem",
          st == 200 and b0.get("rate_limit", {}).get("decision") == "passed", str(st))
    # 本秒已满；连发两个 429 把 t=1003 预约占满
    st, _, ba = catalog_get()
    st, _, bb = catalog_get()
    check("two more 429 land on same next second",
          st == 429 and ba["slot"] == 1003 and bb["slot"] == 1003
          and ba["reservation_id"] != bb["reservation_id"], str((ba, bb))[:200])
    rid_a, rid_b = ba["reservation_id"], bb["reservation_id"]
    # 第三个 429 只能跳到 1004
    st, _, bc = catalog_get()
    check("fully booked next second pushes reservation further",
          st == 429 and bc["slot"] == 1004, str(bc)[:160])
    # 退掉 1003 的一个预约
    st, _, d = s.c.post("/api/ratelimit/cancel", {"reservation_id": rid_a})
    check("cancel future reservation releases slot",
          st == 200 and d["released"] is True and d["future_slot"] is True and d["slot"] == 1003)
    # 再 429 应补回 1003
    st, _, bd = catalog_get()
    check("refill: new reservation reuses freed future second",
          st == 429 and bd["slot"] == 1003, str(bd)[:160])
    # 退已退的号 -> 409
    st, _, b = s.c.post("/api/ratelimit/cancel", {"reservation_id": rid_a})
    check("double cancel -> 409", st == 409 and b.get("error_code") == "reservation_cancelled",
          f"got st={st} b={json.dumps(b, ensure_ascii=False)[:200]}")
    # 退另一个号（同样是未来秒）
    st, _, d = s.c.post("/api/ratelimit/cancel", {"reservation_id": rid_b})
    check("cancel second reservation ok", st == 200 and d["slot"] == 1003)

    # ---- 8.5 最早重试时刻：跳过更早时段；非法值 400 ----
    st, _, b = catalog_get("?earliest_retry=1050")
    check("earliest_retry skips earlier slots", st == 429 and b["slot"] >= 1050, str(b)[:160])
    st, _, b = catalog_get("?earliest_retry=999")
    check("earliest_retry in the past -> 400", st == 400 and b.get("error") == "invalid_earliest_retry")
    st, _, b = catalog_get("?earliest_retry=not-an-int")
    check("non-integer earliest_retry -> 400", st == 400)

    # ---- 8.6 停用路由 -> 503 优先，不发号也不扣预约 ----
    acquire(s, "A")
    st, d = publish(s, "A", 1, "req-rl-disable", ["/api/catalog"])
    s.wait_status(d["run_id"], {"done"})
    n_resv_before = len(rl_state()["reservations"])
    st, h503, b = catalog_get()
    check("disabled catalog route -> 503 before rate limiting",
          st == 503 and b.get("error") == "route_disabled", str(st))
    st2, _, _ = catalog_get()
    check("disabled route consistently 503", st2 == 503)
    n_resv_after = len(rl_state()["reservations"])
    check("503 issues no reservation and consumes nothing",
          n_resv_before == n_resv_after, f"{n_resv_before} -> {n_resv_after}")
    # 重新启用路由
    st, d = publish(s, "A", 1, "req-rl-enable", [])
    s.wait_status(d["run_id"], {"done"})
    st, _, _ = catalog_get()
    check("catalog re-enabled goes through limiter again", st in (200, 429))

    # ---- 8.7 关闭限速：本轮预约全部作废，再开是新 epoch；普通请求直接 200 ----
    epoch_on = rl_state()["epoch"]
    st, _, d = s.c.post("/api/ratelimit/config", {"enabled": False})
    check("disable bumps epoch and reports changed",
          st == 200 and d["enabled"] is False and d["epoch"] == epoch_on + 1)
    rl = rl_state()
    check("no live reservations after switch off",
          all(r["status"] != "reserved" for r in rl["reservations"]))
    st, _, _ = catalog_get()
    check("catalog unrestricted once off", st == 200)
    st, _, b = catalog_get(f"?reservation_id={rid2}")
    check("old reservation rejected after void",
          st == 409 and b.get("error") == "limiter_disabled", str(b)[:160])
    st, _, b = s.c.post("/api/ratelimit/cancel", {"reservation_id": rid2})
    check("cancel while disabled is a no-op release", st == 409)
    st, _, d = s.c.post("/api/ratelimit/config", {"enabled": True})
    check("re-enable starts a fresh epoch", st == 200 and d["epoch"] == epoch_on + 2)
    rl = rl_state()
    check("fresh epoch has no reservations", rl["reservations"] == [])

    # ---- 8.8 连续满秒下找空档 O(1)：区间合并 + 不预建未来秒 ----
    # 语义：普通请求只能用“扣掉已放行和未兑预约后的余量”，不能消费预约占住的名额。
    # 冷启动秒：2 普通放行占满 + 2 个 429 预约下一秒；
    # 后续秒：先凭号兑现上一秒的 2 个预约（占满本秒），普通请求全部 429 并预约下一秒。
    rs0 = [catalog_get() for _ in range(4)]
    check("cold second: 2 pass + 2 limited",
          sorted(st for st, _, _ in rs0) == [200, 200, 429, 429])
    pending = [b["reservation_id"] for st, _, b in rs0 if st == 429]
    s.c.post("/api/clock/advance", {"seconds": 1})

    def saturate_second(sec: int) -> None:
        # 兑现上一秒预约，恰好占满本秒
        for rid in pending:
            rsc, _, rb = catalog_get(f"?reservation_id={rid}")
            check(f"steady second {sec}: reservation redeemed on due second",
                  rsc == 200 and rb.get("rate_limit", {}).get("redeemed") is True,
                  f"{rsc} {rb.get('error')}")
        # 本秒已被兑现占满：两个普通请求都应 429，并预约下一秒
        limited = [catalog_get() for _ in range(2)]
        check(f"steady second {sec}: ordinary requests all limited",
              all(st == 429 for st, _, _ in limited), str([st for st, _, _ in limited]))
        return [b["reservation_id"] for st, _, b in limited if st == 429]

    for sec in range(40):
        pending = saturate_second(sec)
        s.c.post("/api/clock/advance", {"seconds": 1})
    db_path = Path(s.state_dir) / "drill.db"
    ro = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    ro.row_factory = sqlite3.Row
    n_runs = ro.execute("SELECT COUNT(*) AS c FROM rl_full_runs").fetchone()["c"]
    check("41 consecutive full seconds merge into ONE full-run row",
          n_runs == 1, f"runs={n_runs}")
    run = ro.execute("SELECT start_slot, end_slot FROM rl_full_runs").fetchone()
    check("full run covers the whole saturated span",
          run["end_slot"] - run["start_slot"] >= 40, dict(run))
    n_future_passed = ro.execute(
        "SELECT COUNT(*) AS c FROM rl_passed "
        "WHERE slot > (SELECT CAST(v AS INTEGER) FROM kv WHERE k='sim_now')"
    ).fetchone()["c"]
    check("no rl_passed rows pre-created for future seconds", n_future_passed == 0)
    n_future_runs = ro.execute(
        "SELECT COUNT(*) AS c FROM rl_full_runs "
        "WHERE start_slot > (SELECT CAST(v AS INTEGER) FROM kv WHERE k='sim_now')"
    ).fetchone()["c"]
    check("no full-run rows pre-created for future seconds", n_future_runs == 0)
    plan = ro.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM rl_full_runs "
        "WHERE epoch=? AND start_slot<=? AND end_slot>=? LIMIT 1",
        (rl_state()["epoch"], 99999, 99999),
    ).fetchall()
    detail = " ".join(dict(p)["detail"] for p in plan)
    check("free-slot probe uses the covering index (no full scan)",
          "COVERING INDEX" in detail.upper(), detail)
    # 再饱和 40 秒，区间仍然只有 1 行——读取量不随连续满秒数线性增长
    for sec in range(40, 80):
        pending = saturate_second(sec)
        s.c.post("/api/clock/advance", {"seconds": 1})
    n_runs2 = ro.execute("SELECT COUNT(*) AS c FROM rl_full_runs").fetchone()["c"]
    check("81 full seconds still ONE run row (read size independent of saturated length)",
          n_runs2 == 1, f"runs={n_runs2}")
    n_expired = ro.execute(
        "SELECT COUNT(*) AS c FROM rl_reservations WHERE status='expired'"
    ).fetchone()["c"]
    check("no reservation expired during correct steady-state redemption", n_expired == 0)
    ro.close()

    # 远跳未来：未预约秒立即有余量，429 只预约紧邻的下一秒
    s.c.post("/api/clock/advance", {"seconds": 1000})
    far = [catalog_get()[0] for _ in range(3)]
    check("far future unused second: 200,200,429", far == [200, 200, 429], str(far))
    st, _, b = catalog_get()
    check("far future 429 books immediate next second",
          st == 429 and b["retry_after"] == 1)

    # 重置后开关恢复默认关闭
    reset(s)
    check("reset restores default-off limiter", rl_state()["enabled"] is False)
    st, _, _ = catalog_get()
    check("catalog free after reset", st == 200)


def main():
    print(f"项目根: {ROOT}")
    s = Server()
    print(f"测试服务: http://127.0.0.1:{s.port}  tmp={s.tmp}")
    try:
        test_gateway_seed(s)
        test_concurrent_acquire(s)
        test_renew(s)
        test_publish_and_503(s)
        _takeover(s, "stage_half")
        _takeover(s, "before_switch")
        test_idempotent_and_restart(s)
        test_write_failure(s)
        test_catalog_rate_limit(s)
    except Exception:
        import traceback
        traceback.print_exc()
        FAILURES.append("uncaught exception (see traceback)")
        print("\n------ server log tail ------")
        print(s.log_text())
    finally:
        s.stop()

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"\033[31m{len(FAILURES)} 组断言失败\033[0m；通过 {PASS_COUNT} 项")
        for f in FAILURES:
            print(" - " + f)
        sys.exit(1)
    print(f"\033[32m全部通过：{PASS_COUNT} 项断言\033[0m")
    sys.exit(0)


if __name__ == "__main__":
    main()
