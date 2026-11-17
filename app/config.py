"""运行期配置，全部来自环境变量，附带本地开发默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # ---- 源 PostgreSQL ----
    pg_dsn: str = _env(
        "PG_DSN",
        "host=127.0.0.1 port=55432 dbname=postgres user=postgres",
    )
    slot_name: str = _env("REPLICATION_SLOT", "artifacts_history_slot")
    table_name: str = _env("SOURCE_TABLE", "artifacts")
    init_sleep_ms: int = int(_env("CAPTURE_INIT_SLEEP_MS", "0"))

    # ---- 测试钩子（生产保持默认关闭）----
    # 设置后，首次基线创建前由应用自己在源库开一个事务写入该行并保持不提交，
    # 直到基线落盘后才提交：确定性地构造"基线前开始、基线后提交"的事务。
    inflight_hook_no: int = int(_env("TEST_INFLIGHT_ARTIFACT_NO", "0"))
    inflight_hook_sleep_s: float = float(_env("TEST_INFLIGHT_SLEEP_S", "3"))

    # 测试钩子：重建基线已 SQLite 落盘、但 mark_streaming 之前的等待毫秒数，
    # 用于确定性地构造“落盘后、收尾前崩溃”，验证重启只收尾、不产生重复世代。
    rebuild_sleep_after_baseline_ms: int = int(
        _env("TEST_REBUILD_SLEEP_AFTER_BASELINE_MS", "0")
    )

    # ---- 本地历史库 SQLite ----
    sqlite_path: str = _env("SQLITE_PATH", "/tmp/capture/history.db")

    # ---- 复制流参数 ----
    status_interval_s: int = int(_env("REPL_STATUS_INTERVAL_S", "5"))
    reconnect_backoff_s: int = int(_env("REPL_RECONNECT_BACKOFF_S", "2"))

    # ---- 服务 ----
    host: str = _env("WEB_HOST", "0.0.0.0")
    port: int = int(_env("WEB_PORT", "8000"))


settings = Settings()
