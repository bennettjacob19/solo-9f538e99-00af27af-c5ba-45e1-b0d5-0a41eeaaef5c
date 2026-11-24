"""路径与演练参数。所有路径都可用环境变量覆盖，便于容器与自测隔离。"""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 网关读取的 JSON 本地卷（只放 current.json / staging.json 两类正文）
DATA_DIR = Path(os.environ.get("DRILL_DATA_DIR", PROJECT_ROOT / "var" / "config"))
# SQLite 与发布端日志目录
STATE_DIR = Path(os.environ.get("DRILL_STATE_DIR", PROJECT_ROOT / "var" / "state"))
DB_PATH = Path(os.environ.get("DRILL_DB_PATH", STATE_DIR / "drill.db"))

CURRENT_PATH = DATA_DIR / "current.json"
STAGING_PATH = DATA_DIR / "staging.json"

# 模拟时钟初始值与租约 TTL（模拟秒，不是真实秒）
START_NOW = 1000
LEASE_TTL = int(os.environ.get("DRILL_LEASE_TTL", "30"))

# 磁盘配额（字节），统计口径包含“已删除但仍被打开”的文件
DISK_QUOTA_BYTES = int(os.environ.get("DRILL_DISK_QUOTA", str(1024 * 1024)))

# 维护后重试流量限速：开启时每个模拟秒最多放行的 GET /api/catalog 次数
# （普通放行 + 凭号预约重试共享这一配额；预约创建时即预占名额）
CATALOG_RATE_PER_SECOND = int(os.environ.get("DRILL_CATALOG_RATE_PER_SECOND", "2"))
# 限速开关默认关闭；目标路由（与 current.json 里的路由 path 一致）
CATALOG_RATE_ROUTE = "/api/catalog"

HOST = os.environ.get("DRILL_HOST", "0.0.0.0")
PORT = int(os.environ.get("DRILL_PORT", "8000"))

# 三个暂停点（外加不暂停）
PAUSE_POINTS = ("none", "stage_half", "before_switch", "effective_no_response")

# 演练内置路由
SEED_ROUTES = [
    {"path": "/health", "upstream": "mock://backend/health", "note": "健康检查"},
    {"path": "/api/login", "upstream": "mock://backend/login", "note": "登录"},
    {"path": "/api/catalog", "upstream": "mock://backend/catalog", "note": "商品目录"},
    {"path": "/api/checkout", "upstream": "mock://backend/checkout", "note": "下单结算"},
    {"path": "/api/inventory", "upstream": "mock://backend/inventory", "note": "库存"},
    {"path": "/admin/reload", "upstream": "mock://admin/reload", "note": "配置重载"},
]


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
